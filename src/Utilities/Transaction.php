<?php

declare(strict_types=1);

namespace Hibla\AsyncPDO\Utilities;

use Hibla\AsyncPDO\Manager\TransactionManager;
use Hibla\AsyncPDO\Utilities\QueryExecutor;
use Hibla\Promise\Interfaces\PromiseInterface;
use PDO;

use function Hibla\async;

/**
 * Represents an active database transaction with scoped query methods.
 * 
 * This class provides a clean API for executing queries within a transaction context.
 * All queries executed through this object are automatically part of the transaction.
 */
final class Transaction
{
    /**
     * Creates a new Transaction instance.
     *
     * @param PDO $pdo The PDO connection for this transaction
     * @param QueryExecutor $queryExecutor The query executor instance
     * @param TransactionManager $transactionManager The transaction manager instance
     */
    public function __construct(
        private readonly PDO $pdo,
        private readonly QueryExecutor $queryExecutor,
        private readonly TransactionManager $transactionManager
    ) {}

    /**
     * Executes a SELECT query and returns all matching rows.
     *
     * @param  string  $sql  SQL query with optional parameter placeholders
     * @param  array<string|int, mixed>  $params  Parameter values for prepared statement
     * @return PromiseInterface<array<int, array<string, mixed>>> Promise resolving to array of associative arrays
     */
    public function query(string $sql, array $params = []): PromiseInterface
    {
        return async(function () use ($sql, $params): array {
            $result = $this->queryExecutor->executeQuery(
                $this->pdo,
                $sql,
                $params,
                'fetchAll'
            );
            /** @var array<int, array<string, mixed>> */
            return $result;
        });
    }

    /**
     * Executes a SELECT query and returns the first matching row.
     *
     * @param  string  $sql  SQL query with optional parameter placeholders
     * @param  array<string|int, mixed>  $params  Parameter values for prepared statement
     * @return PromiseInterface<array<string, mixed>|false> Promise resolving to associative array or false if no rows
     */
    public function fetchOne(string $sql, array $params = []): PromiseInterface
    {
        return async(function () use ($sql, $params): array|false {
            $result = $this->queryExecutor->executeQuery(
                $this->pdo,
                $sql,
                $params,
                'fetchOne'
            );
            /** @var array<string, mixed>|false */
            return $result;
        });
    }


    /**
     * Executes an INSERT, UPDATE, or DELETE statement and returns affected row count.
     *
     * @param  string  $sql  SQL statement with optional parameter placeholders
     * @param  array<string|int, mixed>  $params  Parameter values for prepared statement
     * @return PromiseInterface<int> Promise resolving to number of affected rows
     */
    public function execute(string $sql, array $params = []): PromiseInterface
    {
        return async(function () use ($sql, $params): int {
            $result = $this->queryExecutor->executeQuery(
                $this->pdo,
                $sql,
                $params,
                'execute'
            );
            /** @var int */
            return $result;
        });
    }
    /**
     * Executes a query and returns a single column value from the first row.
     *
     * @param  string  $sql  SQL query with optional parameter placeholders
     * @param  array<string|int, mixed>  $params  Parameter values for prepared statement
     * @return PromiseInterface<mixed> Promise resolving to scalar value or false if no rows
     */
    public function fetchValue(string $sql, array $params = []): PromiseInterface
    {
        return async(fn() => $this->queryExecutor->executeQuery(
            $this->pdo,
            $sql,
            $params,
            'fetchValue'
        ));
    }

    /**
     * Registers a callback to execute when this transaction commits.
     *
     * The callback will be executed after the transaction successfully commits
     * but before the transaction() method returns.
     *
     * @param  callable(): void  $callback  Callback to execute on commit
     * @return void
     */
    public function onCommit(callable $callback): void
    {
        $this->transactionManager->onCommit($callback);
    }

    /**
     * Registers a callback to execute when this transaction rolls back.
     *
     * The callback will be executed after the transaction is rolled back
     * but before the exception is re-thrown.
     *
     * @param  callable(): void  $callback  Callback to execute on rollback
     * @return void
     */
    public function onRollback(callable $callback): void
    {
        $this->transactionManager->onRollback($callback);
    }

    /**
     * Gets the underlying PDO connection.
     * 
     * Useful for advanced operations or raw PDO access within the transaction.
     *
     * @return PDO The PDO connection
     */
    public function getConnection(): PDO
    {
        return $this->pdo;
    }
}
