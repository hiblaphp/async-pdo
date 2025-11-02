<?php

declare(strict_types=1);

namespace Hibla\AsyncPDO\Manager;

use Hibla\AsyncPDO\Enums\IsolationLevel;
use Hibla\AsyncPDO\Exceptions\NotInTransactionException;
use Hibla\AsyncPDO\Exceptions\TransactionException;
use Hibla\AsyncPDO\Exceptions\TransactionFailedException;
use Hibla\AsyncPDO\Utilities\QueryExecutor;
use Hibla\AsyncPDO\Utilities\Transaction;
use Hibla\Promise\Interfaces\PromiseInterface;
use PDO;
use Throwable;
use WeakMap;

use function Hibla\async;
use function Hibla\await;

/**
 * Manages database transactions and their callbacks.
 * 
 * This class handles transaction lifecycle including begin/commit/rollback operations,
 * retry logic, isolation level management, and execution of registered callbacks.
 */
final class TransactionManager
{
    /** @var WeakMap<PDO, array{commit: list<callable(): void>, rollback: list<callable(): void>, fiber: \Fiber<mixed, mixed, mixed, mixed>|null}> Transaction callbacks using WeakMap */
    private WeakMap $transactionCallbacks;

    /** @var PDO|null Current transaction PDO connection for the active fiber tree */
    private ?PDO $currentTransactionPDO = null;

    /**
     * Creates a new TransactionManager instance.
     */
    public function __construct()
    {
        $this->transactionCallbacks = new WeakMap();
    }

    /**
     * Registers a callback to execute when the current transaction commits.
     *
     * This method can only be called from within an active transaction.
     * The callback will be executed after the transaction successfully commits
     * but before the transaction() method returns.
     *
     * @param  callable(): void  $callback  Callback to execute on commit
     * @return void
     *
     * @throws NotInTransactionException If not currently in a transaction
     * @throws TransactionException If transaction state is corrupted
     */
    public function onCommit(callable $callback): void
    {
        $pdo = $this->getCurrentTransactionPDO();

        if ($pdo === null) {
            throw new NotInTransactionException(
                'onCommit() can only be called within a transaction.'
            );
        }

        if (!isset($this->transactionCallbacks[$pdo])) {
            throw new TransactionException('Transaction state not found.');
        }

        $transactionData = $this->transactionCallbacks[$pdo];
        $commitCallbacks = $transactionData['commit'];
        $commitCallbacks[] = $callback;

        $this->transactionCallbacks[$pdo] = [
            'commit' => $commitCallbacks,
            'rollback' => $transactionData['rollback'],
            'fiber' => $transactionData['fiber'],
        ];
    }

    /**
     * Executes a transaction with retry logic and optional isolation level.
     *
     * Automatically handles transaction begin/commit/rollback. The callback receives
     * a Transaction object for executing queries. If the callback throws an exception,
     * the transaction is rolled back and retried based on the specified number of attempts.
     *
     * Registered onCommit() callbacks are executed after successful commit.
     * Registered onRollback() callbacks are executed after rollback.
     *
     * @param  callable(): PromiseInterface<PDO>  $getConnection  Callback to acquire connection
     * @param  callable(PDO): void  $releaseConnection  Callback to release connection
     * @param  callable(Transaction): mixed  $callback  Transaction callback receiving Transaction object
     * @param  QueryExecutor  $queryExecutor  Query executor instance
     * @param  int  $attempts  Number of times to attempt the transaction
     * @param  IsolationLevel|null  $isolationLevel  Transaction isolation level (optional)
     * @return PromiseInterface<mixed> Promise resolving to callback's return value
     *
     * @throws TransactionFailedException If transaction fails after all attempts
     * @throws \InvalidArgumentException If attempts is less than 1
     */
    public function executeTransaction(
        callable $getConnection,
        callable $releaseConnection,
        callable $callback,
        QueryExecutor $queryExecutor,
        int $attempts,
        ?IsolationLevel $isolationLevel = null
    ): PromiseInterface {
        return async(function () use ($getConnection, $releaseConnection, $callback, $queryExecutor, $attempts, $isolationLevel) {
            if ($attempts < 1) {
                throw new \InvalidArgumentException('Transaction attempts must be at least 1.');
            }

            /** @var Throwable|null $lastException */
            $lastException = null;

            /** @var list<array{attempt: int, error: string, time: float}> */
            $attemptHistory = [];

            for ($currentAttempt = 1; $currentAttempt <= $attempts; $currentAttempt++) {
                $connection = null;
                $startTime = microtime(true);

                try {
                    $connection = await($getConnection());
                    $result = await($this->runTransaction($connection, $callback, $queryExecutor, $isolationLevel));
                    return $result;
                } catch (Throwable $e) {
                    $lastException = $e;

                    $attemptHistory[] = [
                        'attempt' => $currentAttempt,
                        'error' => $e->getMessage(),
                        'time' => microtime(true) - $startTime,
                    ];

                    if ($currentAttempt < $attempts) {
                        continue;
                    }

                    throw new TransactionFailedException(
                        sprintf(
                            'Transaction failed after %d attempt(s): %s',
                            $attempts,
                            $e->getMessage()
                        ),
                        $attempts,
                        $e,
                        $attemptHistory
                    );
                } finally {
                    if ($connection !== null) {
                        $releaseConnection($connection);
                    }
                }
            }

            if ($lastException !== null) {
                throw new TransactionFailedException(
                    sprintf('Transaction failed after %d attempt(s)', $attempts),
                    $attempts,
                    $lastException,
                    $attemptHistory
                );
            }

            throw new TransactionException('Transaction failed without exception.');
        });
    }

    /**
     * Runs a single transaction attempt.
     *
     * Executes BEGIN TRANSACTION, creates Transaction object, runs callback, and either COMMIT or ROLLBACK.
     * Manages transaction state and executes appropriate callbacks.
     *
     * @param  PDO  $connection  PDO connection
     * @param  callable(Transaction): mixed  $callback  Transaction callback receiving Transaction object
     * @param  QueryExecutor  $queryExecutor  Query executor instance
     * @param  IsolationLevel|null  $isolationLevel  Transaction isolation level (optional)
     * @return PromiseInterface<mixed> Promise resolving to callback's return value
     *
     * @throws TransactionException If BEGIN, COMMIT, or isolation level setting fails
     * @throws Throwable If callback throws (after ROLLBACK)
     */
    private function runTransaction(
        PDO $connection,
        callable $callback,
        QueryExecutor $queryExecutor,
        ?IsolationLevel $isolationLevel = null
    ): PromiseInterface {
        return async(function () use ($connection, $callback, $queryExecutor, $isolationLevel) {
            $currentFiber = \Fiber::getCurrent();

            /** @var array{commit: list<callable(): void>, rollback: list<callable(): void>, fiber: \Fiber<mixed, mixed, mixed, mixed>|null} $initialState */
            $initialState = [
                'commit' => [],
                'rollback' => [],
                'fiber' => $currentFiber,
            ];

            $this->transactionCallbacks[$connection] = $initialState;

            $previousTransactionPDO = $this->currentTransactionPDO;
            $this->currentTransactionPDO = $connection;

            try {
                $levelToSet = $isolationLevel ?? $this->getDefaultIsolationLevel($connection);
                if ($connection->getAttribute(PDO::ATTR_DRIVER_NAME) !== 'sqlite') {
                    $this->setIsolationLevel($connection, $levelToSet);
                }

                if (!$connection->beginTransaction()) {
                    throw new TransactionException('Failed to begin transaction');
                }

                $transaction = new Transaction($connection, $queryExecutor, $this);
                $result = $callback($transaction);

                if ($result instanceof PromiseInterface) {
                    $result = await($result);
                }

                if (!$connection->commit()) {
                    throw new TransactionException('Failed to commit transaction');
                }

                $this->executeCallbacks($connection, 'commit');

                return $result;
            } catch (Throwable $e) {
                if ($connection->inTransaction()) {
                    $connection->rollBack();
                }

                $this->executeCallbacks($connection, 'rollback');

                throw $e;
            } finally {
                $this->currentTransactionPDO = $previousTransactionPDO;

                if (isset($this->transactionCallbacks[$connection])) {
                    unset($this->transactionCallbacks[$connection]);
                }
            }
        });
    }

    /**
     * Registers a callback to execute when the current transaction rolls back.
     *
     * This method can only be called from within an active transaction.
     * The callback will be executed after the transaction is rolled back
     * but before the exception is re-thrown.
     *
     * @param  callable(): void  $callback  Callback to execute on rollback
     * @return void
     *
     * @throws NotInTransactionException If not currently in a transaction
     * @throws TransactionException If transaction state is corrupted
     */
    public function onRollback(callable $callback): void
    {
        $pdo = $this->getCurrentTransactionPDO();

        if ($pdo === null) {
            throw new NotInTransactionException(
                'onRollback() can only be called within a transaction.'
            );
        }

        if (!isset($this->transactionCallbacks[$pdo])) {
            throw new TransactionException('Transaction state not found.');
        }

        $transactionData = $this->transactionCallbacks[$pdo];
        $rollbackCallbacks = $transactionData['rollback'];
        $rollbackCallbacks[] = $callback;

        $this->transactionCallbacks[$pdo] = [
            'commit' => $transactionData['commit'],
            'rollback' => $rollbackCallbacks,
            'fiber' => $transactionData['fiber'],
        ];
    }

    /**
     * Gets the current transaction's PDO instance if in a transaction.
     *
     * This method returns the PDO connection for the active transaction context.
     *
     * @return PDO|null Connection instance or null if not in transaction
     */
    public function getCurrentTransactionPDO(): ?PDO
    {
        return $this->currentTransactionPDO;
    }

    /**
     * Executes registered callbacks for commit or rollback.
     *
     * Runs all callbacks registered for the specified transaction event.
     * If any callback throws an exception, execution stops and the first
     * exception is re-thrown after all callbacks have been attempted.
     *
     * @param  PDO  $connection  PDO connection
     * @param  string  $type  'commit' or 'rollback'
     * @return void
     *
     * @throws TransactionException If any callback throws an exception
     */
    private function executeCallbacks(PDO $connection, string $type): void
    {
        if (!isset($this->transactionCallbacks[$connection])) {
            return;
        }

        $transactionData = $this->transactionCallbacks[$connection];

        if ($type !== 'commit' && $type !== 'rollback') {
            return;
        }

        $callbacks = $transactionData[$type];

        /** @var list<Throwable> $exceptions */
        $exceptions = [];

        foreach ($callbacks as $callback) {
            try {
                $callback();
            } catch (Throwable $e) {
                $exceptions[] = $e;
            }
        }

        if (count($exceptions) > 0) {
            throw new TransactionException(
                sprintf(
                    'Transaction %s callback failed: %s',
                    $type,
                    $exceptions[0]->getMessage()
                ),
                0,
                $exceptions[0]
            );
        }
    }

    /**
     * Gets the default isolation level for the database driver.
     *
     * Returns the appropriate default isolation level based on the database type:
     * - MySQL: REPEATABLE READ
     * - PostgreSQL: READ COMMITTED
     * - SQL Server: READ COMMITTED
     * - SQLite: SERIALIZABLE
     *
     * @param  PDO  $connection  PDO connection
     * @return IsolationLevel Default isolation level for the driver
     */
    private function getDefaultIsolationLevel(PDO $connection): IsolationLevel
    {
        $driver = $connection->getAttribute(PDO::ATTR_DRIVER_NAME);

        return match ($driver) {
            'mysql' => IsolationLevel::REPEATABLE_READ,
            'pgsql' => IsolationLevel::READ_COMMITTED,
            'sqlsrv' => IsolationLevel::READ_COMMITTED,
            'sqlite' => IsolationLevel::SERIALIZABLE,
            default => IsolationLevel::READ_COMMITTED,
        };
    }

    /**
     * Sets the transaction isolation level for the connection.
     *
     * This method sets the isolation level using database-specific syntax.
     * The isolation level must be set before BEGIN TRANSACTION is called.
     *
     * @param  PDO  $connection  PDO connection
     * @param  IsolationLevel  $isolationLevel  Desired isolation level
     * @return void
     *
     * @throws TransactionException If setting isolation level fails or database doesn't support it
     */
    private function setIsolationLevel(PDO $connection, IsolationLevel $isolationLevel): void
    {
        $driver = $connection->getAttribute(PDO::ATTR_DRIVER_NAME);

        $sql = match ($driver) {
            'mysql' => "SET SESSION TRANSACTION ISOLATION LEVEL {$isolationLevel->value}",
            'pgsql' => "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL {$isolationLevel->value}",
            'sqlite' => throw new TransactionException('SQLite does not support transaction isolation levels'),
            'sqlsrv' => "SET TRANSACTION ISOLATION LEVEL {$isolationLevel->value}",
            'oci' => throw new TransactionException('Oracle isolation level setting requires ALTER SESSION syntax'),
            default => "SET TRANSACTION ISOLATION LEVEL {$isolationLevel->value}",
        };

        if ($connection->exec($sql) === false) {
            $errorInfo = $connection->errorInfo();
            assert(is_string($isolationLevel->value));
            assert(is_string($errorInfo[2]));
            throw new TransactionException(
                sprintf(
                    'Failed to set isolation level to %s: %s',
                    $isolationLevel->value,
                    $errorInfo[2] ?? 'Unknown error'
                )
            );
        }
    }
}
