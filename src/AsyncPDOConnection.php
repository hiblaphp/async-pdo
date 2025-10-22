<?php

namespace Hibla\AsyncPDO;

use Hibla\AsyncPDO\Manager\PoolManager;
use Hibla\Promise\Interfaces\PromiseInterface;
use PDO;
use Throwable;
use WeakMap;

/**
 * Instance-based Asynchronous PDO API for independent database connections.
 *
 * This class provides non-static methods for managing a single connection pool.
 * Each instance is completely independent, allowing true multi-database support
 * without global state.
 */
final class AsyncPDOConnection
{
    /** @var PoolManager|null Connection pool instance for this connection */
    private ?PoolManager $pool = null;

    /** @var bool Tracks initialization state of this instance */
    private bool $isInitialized = false;

    /** @var WeakMap<PDO, array{commit: list<callable(): void>, rollback: list<callable(): void>, fiber: \Fiber<mixed, mixed, mixed, mixed>|null}>|null Transaction callbacks using WeakMap */
    private ?WeakMap $transactionCallbacks = null;

    /**
     * Creates a new independent AsyncPDOConnection instance.
     *
     * Each instance manages its own connection pool and is completely
     * independent from other instances, allowing true multi-database support.
     *
     * @param  array<string, mixed>  $dbConfig  Database configuration array containing:
     *                                          - driver: Database driver (e.g., 'mysql', 'pgsql')
     *                                          - host: Database host (e.g., 'localhost')
     *                                          - database: Database name
     *                                          - port: Database port
     *                                          - username: Database username
     *                                          - password: Database password
     *                                          - options: PDO options array (optional)
     * @param  int  $poolSize  Maximum number of connections in the pool
     *
     * @throws \InvalidArgumentException If configuration is invalid
     */
    public function __construct(array $dbConfig, int $poolSize = 10)
    {
        $this->pool = new PoolManager($dbConfig, $poolSize);
        $this->transactionCallbacks = new WeakMap();
        $this->isInitialized = true;
    }

    /**
     * Resets this instance, closing all connections and clearing state.
     * After reset, this instance cannot be used until recreated.
     *
     * @return void
     */
    public function reset(): void
    {
        if ($this->pool !== null) {
            $this->pool->close();
        }
        $this->pool = null;
        $this->isInitialized = false;
        $this->transactionCallbacks = null;
    }

    /**
     * Registers a callback to execute when the current transaction commits.
     *
     * @param  callable(): void  $callback  Callback to execute on commit
     * @return void
     *
     * @throws \RuntimeException If not currently in a transaction
     */
    public function onCommit(callable $callback): void
    {
        $pdo = $this->getCurrentTransactionPDO();

        if ($pdo === null) {
            throw new \RuntimeException('onCommit() can only be called within a transaction.');
        }

        if ($this->transactionCallbacks === null || !isset($this->transactionCallbacks[$pdo])) {
            throw new \RuntimeException('Transaction state not found.');
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
     * Registers a callback to execute when the current transaction rolls back.
     *
     * @param  callable(): void  $callback  Callback to execute on rollback
     * @return void
     *
     * @throws \RuntimeException If not currently in a transaction
     */
    public function onRollback(callable $callback): void
    {
        $pdo = $this->getCurrentTransactionPDO();

        if ($pdo === null) {
            throw new \RuntimeException('onRollback() can only be called within a transaction.');
        }

        if ($this->transactionCallbacks === null || !isset($this->transactionCallbacks[$pdo])) {
            throw new \RuntimeException('Transaction state not found.');
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
     * Executes a callback with a connection from this instance's pool.
     *
     * @template TResult
     *
     * @param  callable(PDO): TResult  $callback  Function that receives PDO instance
     * @return PromiseInterface<TResult> Promise resolving to callback's return value
     *
     * @throws \RuntimeException If this instance is not initialized
     */
    public function run(callable $callback): PromiseInterface
    {
        // @phpstan-ignore-next-line
        return async(function () use ($callback): mixed {
            $pdo = null;

            try {
                $pdo = await($this->getPool()->get());
                return $callback($pdo);
            } finally {
                if ($pdo !== null) {
                    $this->getPool()->release($pdo);
                }
            }
        });
    }

    /**
     * Executes a SELECT query and returns all matching rows.
     *
     * @param  string  $sql  SQL query with optional parameter placeholders
     * @param  array<string|int, mixed>  $params  Parameter values for prepared statement
     * @return PromiseInterface<array<int, array<string, mixed>>>
     *
     * @throws \PDOException If query execution fails
     */
    public function query(string $sql, array $params = []): PromiseInterface
    {
        return $this->run(function (PDO $pdo) use ($sql, $params): array {
            $stmt = $pdo->prepare($sql);
            $stmt->execute($params);

            /** @var array<int, array<string, mixed>> */
            $result = $stmt->fetchAll(PDO::FETCH_ASSOC);

            return $result;
        });
    }

    /**
     * Executes a SELECT query and returns the first matching row.
     *
     * @param  string  $sql  SQL query with optional parameter placeholders
     * @param  array<string|int, mixed>  $params  Parameter values for prepared statement
     * @return PromiseInterface<array<string, mixed>|false>
     *
     * @throws \PDOException If query execution fails
     */
    public function fetchFirst(string $sql, array $params = []): PromiseInterface
    {
        return $this->run(function (PDO $pdo) use ($sql, $params): array|false {
            $stmt = $pdo->prepare($sql);
            $stmt->execute($params);

            /** @var array<string, mixed>|false */
            $result = $stmt->fetch(PDO::FETCH_ASSOC);

            return $result;
        });
    }

    /**
     * Executes an INSERT, UPDATE, or DELETE statement and returns affected row count.
     *
     * @param  string  $sql  SQL statement with optional parameter placeholders
     * @param  array<string|int, mixed>  $params  Parameter values for prepared statement
     * @return PromiseInterface<int> Promise resolving to number of affected rows
     *
     * @throws \PDOException If statement execution fails
     */
    public function execute(string $sql, array $params = []): PromiseInterface
    {
        // @phpstan-ignore-next-line
        return $this->run(function (PDO $pdo) use ($sql, $params): int {
            $stmt = $pdo->prepare($sql);
            $stmt->execute($params);

            return $stmt->rowCount();
        });
    }

    /**
     * Executes a query and returns a single column value from the first row.
     *
     * Useful for queries that return a single scalar value like COUNT, MAX, etc.
     *
     * @param  string  $sql  SQL query with optional parameter placeholders
     * @param  array<string|int, mixed>  $params  Parameter values for prepared statement
     * @return PromiseInterface<mixed> Promise resolving to scalar value or false if no rows
     *
     * @throws \PDOException If query execution fails
     */
    public function fetchValue(string $sql, array $params = []): PromiseInterface
    {
        return $this->run(function (PDO $pdo) use ($sql, $params): mixed {
            $stmt = $pdo->prepare($sql);
            $stmt->execute($params);

            return $stmt->fetch(PDO::FETCH_COLUMN);
        });
    }

    /**
     * Executes multiple operations within a database transaction.
     *
     * Automatically handles transaction begin/commit/rollback. If the callback
     * throws an exception, the transaction is rolled back and retried based on
     * the specified number of attempts.
     *
     * @param  callable(PDO): mixed  $callback  Transaction callback receiving PDO instance
     * @param  int  $attempts  Number of times to attempt the transaction (default: 1)
     * @return PromiseInterface<mixed> Promise resolving to callback's return value
     *
     * @throws \PDOException If transaction operations fail after all attempts
     * @throws Throwable Any exception thrown by the callback after all attempts
     */
    public function transaction(callable $callback, int $attempts = 1): PromiseInterface
    {
        return async(function () use ($callback, $attempts) {
            if ($attempts < 1) {
                throw new \InvalidArgumentException('Transaction attempts must be at least 1.');
            }

            /** @var Throwable|null $lastException */
            $lastException = null;

            for ($currentAttempt = 1; $currentAttempt <= $attempts; $currentAttempt++) {
                try {
                    return await($this->run(function (PDO $pdo) use ($callback) {
                        $currentFiber = \Fiber::getCurrent();

                        $this->ensureTransactionCallbacksInitialized();

                        /** @var array{commit: list<callable(): void>, rollback: list<callable(): void>, fiber: \Fiber<mixed, mixed, mixed, mixed>|null} $initialState */
                        $initialState = [
                            'commit' => [],
                            'rollback' => [],
                            'fiber' => $currentFiber,
                        ];

                        // @phpstan-ignore-next-line
                        $this->transactionCallbacks[$pdo] = $initialState;

                        $pdo->beginTransaction();

                        try {
                            $result = $callback($pdo);
                            $pdo->commit();

                            $this->executeCallbacks($pdo, 'commit');

                            return $result;
                        } catch (Throwable $e) {
                            if ($pdo->inTransaction()) {
                                $pdo->rollBack();
                            }

                            $this->executeCallbacks($pdo, 'rollback');

                            throw $e;
                        } finally {
                            if (isset($this->transactionCallbacks[$pdo])) {
                                unset($this->transactionCallbacks[$pdo]);
                            }
                        }
                    }));
                } catch (Throwable $e) {
                    $lastException = $e;

                    if ($currentAttempt < $attempts) {
                        continue;
                    }

                    throw $e;
                }
            }

            if ($lastException !== null) {
                throw $lastException;
            }

            throw new \RuntimeException('Transaction failed without exception.');
        });
    }

    /**
     * Gets statistics about this instance's connection pool.
     *
     * @return array<string, int|bool> Pool statistics
     */
    public function getStats(): array
    {
        return $this->getPool()->getStats();
    }

    /**
     * Gets the most recently used connection from this pool.
     *
     * @return PDO|null The last connection or null if none used yet
     */
    public function getLastConnection(): ?PDO
    {
        return $this->getPool()->getLastConnection();
    }

    /**
     * Gets the current transaction's PDO instance if in a transaction within the current fiber.
     *
     * @return PDO|null PDO instance or null if not in transaction
     *
     * @internal This method is for internal use only
     */
    private function getCurrentTransactionPDO(): ?PDO
    {
        if ($this->transactionCallbacks === null) {
            return null;
        }

        $currentFiber = \Fiber::getCurrent();

        foreach ($this->transactionCallbacks as $pdo => $data) {
            if ($data['fiber'] === $currentFiber) {
                return $pdo;
            }
        }

        return null;
    }

    /**
     * Executes registered callbacks for commit or rollback.
     *
     * @param  PDO  $pdo  PDO instance
     * @param  string  $type  'commit' or 'rollback'
     * @return void
     *
     * @throws Throwable If any callback throws an exception
     *
     * @internal This method is for internal use only
     */
    private function executeCallbacks(PDO $pdo, string $type): void
    {
        if ($this->transactionCallbacks === null || !isset($this->transactionCallbacks[$pdo])) {
            return;
        }

        $transactionData = $this->transactionCallbacks[$pdo];

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
            throw $exceptions[0];
        }
    }

    /**
     * Ensures the transaction callbacks WeakMap is initialized.
     *
     * @return void
     *
     * @internal This method is for internal use only
     */
    private function ensureTransactionCallbacksInitialized(): void
    {
        if ($this->transactionCallbacks === null) {
            $this->transactionCallbacks = new WeakMap();
        }
    }

    /**
     * Gets the connection pool instance.
     *
     * @return PoolManager The initialized connection pool
     *
     * @throws \RuntimeException If this instance is not initialized
     *
     * @internal This method is for internal use only
     */
    private function getPool(): PoolManager
    {
        if (!$this->isInitialized || $this->pool === null) {
            throw new \RuntimeException(
                'AsyncPDOConnection instance has not been initialized.'
            );
        }

        return $this->pool;
    }
}