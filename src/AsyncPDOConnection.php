<?php

declare(strict_types=1);

namespace Hibla\AsyncPDO;

use Hibla\AsyncPDO\Enums\IsolationLevel;
use Hibla\AsyncPDO\Exceptions\NotInTransactionException;
use Hibla\AsyncPDO\Exceptions\NotInitializedException;
use Hibla\AsyncPDO\Exceptions\QueryException;
use Hibla\AsyncPDO\Exceptions\TransactionException;
use Hibla\AsyncPDO\Exceptions\TransactionFailedException;
use Hibla\AsyncPDO\Manager\PoolManager;
use Hibla\AsyncPDO\Manager\TransactionManager;
use Hibla\AsyncPDO\Utilities\QueryExecutor;
use Hibla\AsyncPDO\Utilities\Transaction;
use Hibla\Promise\Interfaces\PromiseInterface;
use PDO;

use function Hibla\async;
use function Hibla\await;

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

    /** @var QueryExecutor|null Handles query execution and result processing */
    private ?QueryExecutor $queryExecutor = null;

    /** @var TransactionManager|null Manages transactions and callbacks */
    private ?TransactionManager $transactionManager = null;

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
        $this->queryExecutor = new QueryExecutor();
        $this->transactionManager = new TransactionManager();
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
        $this->queryExecutor = null;
        $this->transactionManager = null;
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
        $this->getTransactionManager()->onCommit($callback);
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
        $this->getTransactionManager()->onRollback($callback);
    }

    /**
     * Executes a callback with a connection from this instance's pool.
     *
     * Automatically handles connection acquisition and release. The callback
     * receives a PDO instance and can perform any database operations.
     * The connection is guaranteed to be released back to the pool even if
     * the callback throws an exception.
     *
     * @template TResult
     *
     * @param  callable(PDO): TResult  $callback  Function that receives PDO instance
     * @return PromiseInterface<TResult> Promise resolving to callback's return value
     *
     * @throws NotInitializedException If this instance is not initialized
     */
    public function run(callable $callback): PromiseInterface
    {
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
     * The query is executed using PDO's prepared statement API.
     * Parameters are safely bound using prepared statements to prevent SQL injection.
     *
     * @param  string  $sql  SQL query with optional parameter placeholders
     * @param  array<string|int, mixed>  $params  Parameter values for prepared statement
     * @return PromiseInterface<array<int, array<string, mixed>>> Promise resolving to array of associative arrays
     *
     * @throws NotInitializedException If this instance is not initialized
     * @throws QueryException If query execution fails
     */
    public function query(string $sql, array $params = []): PromiseInterface
    {
        /** @var PromiseInterface<array<int, array<string, mixed>>> */
        return $this->executeQuery($sql, $params, 'fetchAll');
    }

    /**
     * Executes a SELECT query and returns the first matching row.
     *
     * The query is executed using PDO's prepared statement API.
     * Returns false if no rows match the query.
     *
     * @param  string  $sql  SQL query with optional parameter placeholders
     * @param  array<string|int, mixed>  $params  Parameter values for prepared statement
     * @return PromiseInterface<array<string, mixed>|false> Promise resolving to associative array or false if no rows
     *
     * @throws NotInitializedException If this instance is not initialized
     * @throws QueryException If query execution fails
     */
    public function fetchOne(string $sql, array $params = []): PromiseInterface
    {
        /** @var PromiseInterface<array<string, mixed>|false> */
        return $this->executeQuery($sql, $params, 'fetchOne');
    }

    /**
     * Executes an INSERT, UPDATE, or DELETE statement and returns affected row count.
     *
     * The statement is executed using PDO's prepared statement API.
     * Returns the number of rows affected by the operation.
     *
     * @param  string  $sql  SQL statement with optional parameter placeholders
     * @param  array<string|int, mixed>  $params  Parameter values for prepared statement
     * @return PromiseInterface<int> Promise resolving to number of affected rows
     *
     * @throws NotInitializedException If this instance is not initialized
     * @throws QueryException If statement execution fails
     */
    public function execute(string $sql, array $params = []): PromiseInterface
    {
        /** @var PromiseInterface<int> */
        return $this->executeQuery($sql, $params, 'execute');
    }

    /**
     * Executes a query and returns a single column value from the first row.
     *
     * Useful for queries that return a single scalar value like COUNT, MAX, etc.
     * Returns false if the query returns no rows.
     *
     * @param  string  $sql  SQL query with optional parameter placeholders
     * @param  array<string|int, mixed>  $params  Parameter values for prepared statement
     * @return PromiseInterface<mixed> Promise resolving to scalar value or false if no rows
     *
     * @throws NotInitializedException If this instance is not initialized
     * @throws QueryException If query execution fails
     */
    public function fetchValue(string $sql, array $params = []): PromiseInterface
    {
        return $this->executeQuery($sql, $params, 'fetchValue');
    }

    /**
     * Executes multiple operations within a database transaction.
     *
     * Automatically handles transaction begin/commit/rollback. The callback receives
     * a Transaction object for executing queries within the transaction context.
     * If the callback throws an exception, the transaction is rolled back and retried
     * based on the specified number of attempts.
     *
     * Registered onCommit() callbacks are executed after successful commit.
     * Registered onRollback() callbacks are executed after rollback.
     *
     * @param  callable(Transaction): mixed  $callback  Transaction callback receiving Transaction object
     * @param  int  $attempts  Number of times to attempt the transaction (default: 1)
     * @param  IsolationLevel|null  $isolationLevel  Transaction isolation level (optional)
     * @return PromiseInterface<mixed> Promise resolving to callback's return value
     *
     * @throws NotInitializedException If this instance is not initialized
     * @throws TransactionFailedException If transaction fails after all attempts
     * @throws \InvalidArgumentException If attempts is less than 1
     */
    public function transaction(
        callable $callback,
        int $attempts = 1,
        ?IsolationLevel $isolationLevel = null
    ): PromiseInterface {
        return $this->getTransactionManager()->executeTransaction(
            fn() => $this->getPool()->get(),
            fn($connection) => $this->getPool()->release($connection),
            $callback,
            $this->getQueryExecutor(),
            $attempts,
            $isolationLevel
        );
    }

    /**
     * Gets statistics about this instance's connection pool.
     *
     * Returns information about the current state of the connection pool,
     * including total connections, available connections, and connections in use.
     *
     * @return array<string, int|bool> Pool statistics including:
     *                                  - total: Total number of connections in pool
     *                                  - available: Number of available connections
     *                                  - inUse: Number of connections currently in use
     *
     * @throws NotInitializedException If this instance is not initialized
     */
    public function getStats(): array
    {
        return $this->getPool()->getStats();
    }

    /**
     * Gets the most recently used connection from this pool.
     *
     * This is primarily useful for debugging and testing purposes.
     * Returns null if no connection has been used yet.
     *
     * @return PDO|null The last connection or null if none used yet
     *
     * @throws NotInitializedException If this instance is not initialized
     */
    public function getLastConnection(): ?PDO
    {
        return $this->getPool()->getLastConnection();
    }

    /**
     * Executes a query with the specified result processing type.
     *
     * This method handles the complete lifecycle of query execution including
     * connection acquisition, query execution, and connection release.
     * 
     * If called from within a transaction, it reuses the transaction's connection.
     *
     * @param  string  $sql  SQL query/statement
     * @param  array<string|int, mixed>  $params  Query parameters
     * @param  string  $resultType  Type of result processing ('fetchAll', 'fetchOne', 'execute', 'fetchValue')
     * @return PromiseInterface<mixed> Promise resolving to processed result
     *
     * @throws NotInitializedException If this instance is not initialized
     * @throws QueryException If query execution fails
     */
    private function executeQuery(string $sql, array $params, string $resultType): PromiseInterface
    {
        return async(function () use ($sql, $params, $resultType) {
            $transactionPdo = $this->getTransactionManager()->getCurrentTransactionPDO();

            if ($transactionPdo !== null) {
                return $this->getQueryExecutor()->executeQuery(
                    $transactionPdo,
                    $sql,
                    $params,
                    $resultType
                );
            }

            $pdo = await($this->getPool()->get());

            try {
                return $this->getQueryExecutor()->executeQuery(
                    $pdo,
                    $sql,
                    $params,
                    $resultType
                );
            } finally {
                $this->getPool()->release($pdo);
            }
        });
    }

    /**
     * Gets the connection pool instance.
     *
     * @return PoolManager The initialized connection pool
     *
     * @throws NotInitializedException If this instance is not initialized
     */
    private function getPool(): PoolManager
    {
        if (!$this->isInitialized || $this->pool === null) {
            throw new NotInitializedException(
                'AsyncPDOConnection instance has not been initialized or has been reset.'
            );
        }

        return $this->pool;
    }

    /**
     * Gets the query executor instance.
     *
     * @return QueryExecutor The initialized query executor
     *
     * @throws NotInitializedException If this instance is not initialized
     */
    private function getQueryExecutor(): QueryExecutor
    {
        if ($this->queryExecutor === null) {
            throw new NotInitializedException(
                'AsyncPDOConnection instance has not been initialized or has been reset.'
            );
        }

        return $this->queryExecutor;
    }

    /**
     * Gets the transaction manager instance.
     *
     * @return TransactionManager The initialized transaction manager
     *
     * @throws NotInitializedException If this instance is not initialized
     */
    private function getTransactionManager(): TransactionManager
    {
        if ($this->transactionManager === null) {
            throw new NotInitializedException(
                'AsyncPDOConnection instance has not been initialized or has been reset.'
            );
        }

        return $this->transactionManager;
    }
}
