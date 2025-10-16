<?php

namespace Hibla\AsyncPDO;

use Hibla\AsyncPDO\Manager\PoolManager;
use Hibla\Promise\Interfaces\PromiseInterface;
use PDO;
use Throwable;
use WeakMap;

/**
 * Asynchronous PDO API providing fiber-based database operations.
 *
 * This class serves as the main entry point for async database operations,
 * managing connection pooling and providing convenient methods for common
 * database tasks like queries, transactions, and batch operations.
 */
final class AsyncPDO
{
    /** @var PoolManager|null Connection pool instance */
    private static ?PoolManager $pool = null;

    /** @var bool Tracks initialization state */
    private static bool $isInitialized = false;

    /** @var WeakMap<PDO, array{commit: list<callable(): void>, rollback: list<callable(): void>, fiber: \Fiber<mixed, mixed, mixed, mixed>|null}>|null Transaction callbacks using WeakMap */
    private static ?WeakMap $transactionCallbacks = null;

    /**
     * Initializes the entire async database system.
     *
     * This is the single point of configuration and must be called before
     * using any other AsyncPDO methods. Multiple calls are ignored.
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
     */
    public static function init(array $dbConfig, int $poolSize = 10): void
    {
        if (self::$isInitialized) {
            return;
        }

        self::$pool = new PoolManager($dbConfig, $poolSize);
        self::$transactionCallbacks = new WeakMap();
        self::$isInitialized = true;
    }

    /**
     * Resets both this facade and the underlying event loop for clean testing.
     *
     * Closes all database connections and clears the pool. Primarily used
     * in testing scenarios to ensure clean state between tests.
     */
    public static function reset(): void
    {
        if (self::$pool !== null) {
            self::$pool->close();
        }
        self::$pool = null;
        self::$isInitialized = false;
        self::$transactionCallbacks = null;
    }

    /**
     * Registers a callback to execute when the current transaction commits.
     *
     * @param  callable(): void  $callback  Callback to execute on commit
     * @return void
     *
     * @throws \RuntimeException If not currently in a transaction or if AsyncPDO is not initialized
     */
    public static function onCommit(callable $callback): void
    {
        $pdo = self::getCurrentTransactionPDO();

        if ($pdo === null) {
            throw new \RuntimeException('onCommit() can only be called within a transaction.');
        }

        if (self::$transactionCallbacks === null || !isset(self::$transactionCallbacks[$pdo])) {
            throw new \RuntimeException('Transaction state not found.');
        }

        $transactionData = self::$transactionCallbacks[$pdo];
        $commitCallbacks = $transactionData['commit'];
        $commitCallbacks[] = $callback;

        self::$transactionCallbacks[$pdo] = [
            'commit' => $commitCallbacks,
            'rollback' => $transactionData['rollback'],
            'fiber' => $transactionData['fiber']
        ];
    }

    /**
     * Registers a callback to execute when the current transaction rolls back.
     *
     * @param  callable(): void  $callback  Callback to execute on rollback
     * @return void
     *
     * @throws \RuntimeException If not currently in a transaction or if AsyncPDO is not initialized
     */
    public static function onRollback(callable $callback): void
    {
        $pdo = self::getCurrentTransactionPDO();

        if ($pdo === null) {
            throw new \RuntimeException('onRollback() can only be called within a transaction.');
        }

        if (self::$transactionCallbacks === null || !isset(self::$transactionCallbacks[$pdo])) {
            throw new \RuntimeException('Transaction state not found.');
        }

        $transactionData = self::$transactionCallbacks[$pdo];
        $rollbackCallbacks = $transactionData['rollback'];
        $rollbackCallbacks[] = $callback;

        self::$transactionCallbacks[$pdo] = [
            'commit' => $transactionData['commit'],
            'rollback' => $rollbackCallbacks,
            'fiber' => $transactionData['fiber']
        ];
    }

    /**
     * Executes a callback with an async PDO connection from the pool.
     *
     * Automatically handles connection acquisition and release. The callback
     * receives a PDO instance and can perform any database operations.
     *
     * @template TResult
     *
     * @param  callable(PDO): TResult  $callback  Function that receives PDO instance
     * @return PromiseInterface<TResult> Promise resolving to callback's return value
     *
     * @throws \RuntimeException If AsyncPDO is not initialized
     */
    public static function run(callable $callback): PromiseInterface
    {
        // @phpstan-ignore-next-line
        return async(function () use ($callback): mixed {
            $pdo = null;

            try {
                $pdo = await(self::getPool()->get());

                return $callback($pdo);
            } finally {
                if ($pdo !== null) {
                    self::getPool()->release($pdo);
                }
            }
        });
    }

    /**
     * Executes a SELECT query and returns all matching rows.
     *
     * @param  string  $sql  SQL query with optional parameter placeholders
     * @param  array<string|int, mixed>  $params  Parameter values for prepared statement
     * @return PromiseInterface<array<int, array<string, mixed>>> Promise resolving to array of associative arrays
     *
     * @throws \RuntimeException If AsyncPDO is not initialized
     * @throws \PDOException If query execution fails
     */
    public static function query(string $sql, array $params = []): PromiseInterface
    {
        return self::run(function (PDO $pdo) use ($sql, $params): array {
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
     * @return PromiseInterface<array<string, mixed>|false> Promise resolving to associative array or false if no rows
     *
     * @throws \RuntimeException If AsyncPDO is not initialized
     * @throws \PDOException If query execution fails
     */
    public static function fetchOne(string $sql, array $params = []): PromiseInterface
    {
        return self::run(function (PDO $pdo) use ($sql, $params): array|false {
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
     * @throws \RuntimeException If AsyncPDO is not initialized
     * @throws \PDOException If statement execution fails
     */
    public static function execute(string $sql, array $params = []): PromiseInterface
    {
        // @phpstan-ignore-next-line
        return self::run(function (PDO $pdo) use ($sql, $params): int {
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
     * @throws \RuntimeException If AsyncPDO is not initialized
     * @throws \PDOException If query execution fails
     */
    public static function fetchValue(string $sql, array $params = []): PromiseInterface
    {
        return self::run(function (PDO $pdo) use ($sql, $params): mixed {
            $stmt = $pdo->prepare($sql);
            $stmt->execute($params);

            return $stmt->fetch(PDO::FETCH_COLUMN);
        });
    }

    /**
     * Executes multiple operations within a database transaction.
     *
     * Automatically handles transaction begin/commit/rollback. If the callback
     * throws an exception, the transaction is rolled back automatically and
     * retried up to the specified number of attempts.
     *
     * @param  callable(PDO): mixed  $callback  Transaction callback receiving PDO instance
     * @param  int  $attempts  Number of times to attempt the transaction (default: 1)
     * @return PromiseInterface<mixed> Promise resolving to callback's return value
     *
     * @throws \RuntimeException If AsyncPDO is not initialized
     * @throws \PDOException If transaction operations fail after all attempts
     * @throws Throwable Any exception thrown by the callback after all attempts (after rollback)
     */
    public static function transaction(callable $callback, int $attempts = 1): PromiseInterface
    {
        return async(function () use ($callback, $attempts) {
            if ($attempts < 1) {
                throw new \InvalidArgumentException('Transaction attempts must be at least 1.');
            }

            /** @var Throwable|null $lastException */
            $lastException = null;

            for ($currentAttempt = 1; $currentAttempt <= $attempts; $currentAttempt++) {
                try {
                    return await(self::run(function (PDO $pdo) use ($callback) {
                        $currentFiber = \Fiber::getCurrent();

                        self::ensureTransactionCallbacksInitialized();

                        /** @var array{commit: list<callable(): void>, rollback: list<callable(): void>, fiber: \Fiber<mixed, mixed, mixed, mixed>|null} $initialState */
                        $initialState = [
                            'commit' => [],
                            'rollback' => [],
                            'fiber' => $currentFiber
                        ];

                        // @phpstan-ignore-next-line
                        self::$transactionCallbacks[$pdo] = $initialState;

                        $pdo->beginTransaction();

                        try {
                            $result = $callback($pdo);
                            $pdo->commit();

                            self::executeCallbacks($pdo, 'commit');

                            return $result;
                        } catch (Throwable $e) {
                            if ($pdo->inTransaction()) {
                                $pdo->rollBack();
                            }

                            self::executeCallbacks($pdo, 'rollback');

                            throw $e;
                        } finally {
                            if (isset(self::$transactionCallbacks[$pdo])) {
                                unset(self::$transactionCallbacks[$pdo]);
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
     * Gets the current transaction's PDO instance if in a transaction within the current fiber.
     *
     * @return PDO|null PDO instance or null if not in transaction
     *
     * @internal This method is for internal use only
     */
    private static function getCurrentTransactionPDO(): ?PDO
    {
        if (self::$transactionCallbacks === null) {
            return null;
        }

        $currentFiber = \Fiber::getCurrent();

        // Iterate through all active transactions to find the one in the current fiber
        foreach (self::$transactionCallbacks as $pdo => $data) {
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
    private static function executeCallbacks(PDO $pdo, string $type): void
    {
        if (self::$transactionCallbacks === null || !isset(self::$transactionCallbacks[$pdo])) {
            return;
        }

        $transactionData = self::$transactionCallbacks[$pdo];

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
    private static function ensureTransactionCallbacksInitialized(): void
    {
        if (self::$transactionCallbacks === null) {
            self::$transactionCallbacks = new WeakMap();
        }
    }

    /**
     * Gets the connection pool instance.
     *
     * @return PoolManager The initialized connection pool
     *
     * @throws \RuntimeException If AsyncPDO has not been initialized
     *
     * @internal This method is for internal use only
     */
    public static function getPool(): PoolManager
    {
        if (! self::$isInitialized || self::$pool === null) {
            throw new \RuntimeException(
                'AsyncPDO has not been initialized. Please call AsyncPDO::init() at application startup.'
            );
        }

        return self::$pool;
    }
}
