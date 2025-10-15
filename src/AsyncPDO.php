<?php

namespace Hibla\AsyncPDO;

use Hibla\AsyncPDO\Manager\PoolManager;
use Hibla\Promise\Interfaces\PromiseInterface;
use PDO;
use Throwable;

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

            $lastException = null;

            for ($currentAttempt = 1; $currentAttempt <= $attempts; $currentAttempt++) {
                try {
                    return await(self::run(function (PDO $pdo) use ($callback) {
                        $pdo->beginTransaction();

                        try {
                            $result = $callback($pdo);
                            $pdo->commit();

                            return $result;
                        } catch (Throwable $e) {
                            $pdo->rollBack();

                            throw $e;
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

            throw $lastException;
        });
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
