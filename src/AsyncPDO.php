<?php

namespace Hibla\AsyncPDO;

use Hibla\AsyncPDO\Exceptions\AsyncPDOException;
use Hibla\Promise\Interfaces\PromiseInterface;
use PDO;

/**
 * Asynchronous PDO API providing fiber-based database operations.
 *
 * This class serves as a singleton facade over AsyncPDOConnection,
 * providing convenient static methods for common database tasks in
 * single-database applications.
 */
final class AsyncPDO
{
    /** @var AsyncPDOConnection|null Underlying connection instance */
    private static ?AsyncPDOConnection $instance = null;

    /** @var bool Tracks initialization state */
    private static bool $isInitialized = false;

    /**
     * Initializes the async database system.
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

        self::$instance = new AsyncPDOConnection($dbConfig, $poolSize);
        self::$isInitialized = true;
    }

    /**
     * Resets the singleton instance for clean testing.
     *
     * Closes all database connections and clears the pool. Primarily used
     * in testing scenarios to ensure clean state between tests.
     */
    public static function reset(): void
    {
        if (self::$instance !== null) {
            self::$instance->reset();
        }
        self::$instance = null;
        self::$isInitialized = false;
    }

    /**
     * Registers a callback to execute when the current transaction commits.
     *
     * @param  callable(): void  $callback  Callback to execute on commit
     *
     * @throws AsyncPDOException If not currently in a transaction or if AsyncPDO is not initialized
     */
    public static function onCommit(callable $callback): void
    {
        self::getInstance()->onCommit($callback);
    }

    /**
     * Registers a callback to execute when the current transaction rolls back.
     *
     * @param  callable(): void  $callback  Callback to execute on rollback
     *
     * @throws AsyncPDOException If not currently in a transaction or if AsyncPDO is not initialized
     */
    public static function onRollback(callable $callback): void
    {
        self::getInstance()->onRollback($callback);
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
     * @throws AsyncPDOException If AsyncPDO is not initialized
     */
    public static function run(callable $callback): PromiseInterface
    {
        return self::getInstance()->run($callback);
    }

    /**
     * Executes a SELECT query and returns all matching rows.
     *
     * @param  string  $sql  SQL query with optional parameter placeholders
     * @param  array<string|int, mixed>  $params  Parameter values for prepared statement
     * @return PromiseInterface<array<int, array<string, mixed>>> Promise resolving to array of associative arrays
     *
     * @throws AsyncPDOException If AsyncPDO is not initialized
     * @throws \PDOException If query execution fails
     */
    public static function query(string $sql, array $params = []): PromiseInterface
    {
        return self::getInstance()->query($sql, $params);
    }

    /**
     * Executes a SELECT query and returns the first matching row.
     *
     * @param  string  $sql  SQL query with optional parameter placeholders
     * @param  array<string|int, mixed>  $params  Parameter values for prepared statement
     * @return PromiseInterface<array<string, mixed>|false> Promise resolving to associative array or false if no rows
     *
     * @throws AsyncPDOException If AsyncPDO is not initialized
     * @throws \PDOException If query execution fails
     */
    public static function fetchOne(string $sql, array $params = []): PromiseInterface
    {
        return self::getInstance()->fetchOne($sql, $params);
    }

    /**
     * Executes an INSERT, UPDATE, or DELETE statement and returns affected row count.
     *
     * @param  string  $sql  SQL statement with optional parameter placeholders
     * @param  array<string|int, mixed>  $params  Parameter values for prepared statement
     * @return PromiseInterface<int> Promise resolving to number of affected rows
     *
     * @throws AsyncPDOException If AsyncPDO is not initialized
     * @throws \PDOException If statement execution fails
     */
    public static function execute(string $sql, array $params = []): PromiseInterface
    {
        return self::getInstance()->execute($sql, $params);
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
     * @throws AsyncPDOException If AsyncPDO is not initialized
     * @throws \PDOException If query execution fails
     */
    public static function fetchValue(string $sql, array $params = []): PromiseInterface
    {
        return self::getInstance()->fetchValue($sql, $params);
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
     * @throws AsyncPDOException If AsyncPDO is not initialized
     * @throws \PDOException If transaction operations fail after all attempts
     * @throws \Throwable Any exception thrown by the callback after all attempts (after rollback)
     */
    public static function transaction(callable $callback, int $attempts = 1): PromiseInterface
    {
        return self::getInstance()->transaction($callback, $attempts);
    }

    /**
     * Gets the underlying AsyncPDOConnection instance.
     *
     * @return AsyncPDOConnection The initialized connection instance
     *
     * @throws AsyncPDOException If AsyncPDO has not been initialized
     *
     * @internal This method is for internal use only
     */
    private static function getInstance(): AsyncPDOConnection
    {
        if (!self::$isInitialized || self::$instance === null) {
            throw new AsyncPDOException(
                'AsyncPDO has not been initialized. Please call AsyncPDO::init() at application startup.'
            );
        }

        return self::$instance;
    }

    /**
     * Gets statistics about the connection pool.
     *
     * @return array<string, int|bool> Pool statistics
     *
     * @throws AsyncPDOException If AsyncPDO has not been initialized
     */
    public static function getStats(): array
    {
        return self::getInstance()->getStats();
    }

    /**
     * Gets the most recently used connection from the pool.
     *
     * @return PDO|null The last connection or null if none used yet
     *
     * @throws AsyncPDOException If AsyncPDO has not been initialized
     */
    public static function getLastConnection(): ?PDO
    {
        return self::getInstance()->getLastConnection();
    }
}
