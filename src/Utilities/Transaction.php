<?php

declare(strict_types=1);

namespace Hibla\AsyncPDO\Utilities;

use Hibla\AsyncPDO\Exceptions\QueryException;
use Hibla\AsyncPDO\Manager\TransactionManager;
use PDO;
use PDOException;
use PDOStatement;

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
        // @phpstan-ignore-next-line
        private readonly QueryExecutor $queryExecutor,
        private readonly TransactionManager $transactionManager
    ) {
    }

    /**
     * Executes a SELECT query and returns all matching rows.
     *
     * @param  string  $sql  SQL query with optional parameter placeholders
     * @param  array<string|int, mixed>  $params  Parameter values for prepared statement
     * @return array<int, array<string, mixed>> Array of associative arrays
     *
     * @throws QueryException If query execution fails
     */
    public function query(string $sql, array $params = []): array
    {
        $stmt = $this->prepareAndExecute($sql, $params);
        $result = $stmt->fetchAll(PDO::FETCH_ASSOC);

        return $result === false ? [] : $result;
    }

    /**
     * Executes a SELECT query and returns the first matching row.
     *
     * @param  string  $sql  SQL query with optional parameter placeholders
     * @param  array<string|int, mixed>  $params  Parameter values for prepared statement
     * @return array<string, mixed>|false Associative array or false if no rows
     *
     * @throws QueryException If query execution fails
     */
    public function fetchOne(string $sql, array $params = []): array|false
    {
        $stmt = $this->prepareAndExecute($sql, $params);
        $result = $stmt->fetch(PDO::FETCH_ASSOC);

        return $result === false ? false : $result;
    }

    /**
     * Executes an INSERT, UPDATE, or DELETE statement and returns affected row count.
     *
     * @param  string  $sql  SQL statement with optional parameter placeholders
     * @param  array<string|int, mixed>  $params  Parameter values for prepared statement
     * @return int Number of affected rows
     *
     * @throws QueryException If statement execution fails
     */
    public function execute(string $sql, array $params = []): int
    {
        $stmt = $this->prepareAndExecute($sql, $params);

        return $stmt->rowCount();
    }

    /**
     * Executes a query and returns a single column value from the first row.
     *
     * @param  string  $sql  SQL query with optional parameter placeholders
     * @param  array<string|int, mixed>  $params  Parameter values for prepared statement
     * @return mixed Scalar value or false if no rows
     *
     * @throws QueryException If query execution fails
     */
    public function fetchValue(string $sql, array $params = []): mixed
    {
        $stmt = $this->prepareAndExecute($sql, $params);

        return $stmt->fetchColumn();
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

    /**
     * Prepares and executes a SQL statement with parameters.
     *
     * @param  string  $sql  SQL query/statement
     * @param  array<string|int, mixed>  $params  Query parameters
     * @return PDOStatement The executed statement
     *
     * @throws QueryException If preparation or execution fails
     */
    private function prepareAndExecute(string $sql, array $params): PDOStatement
    {
        try {
            $pdo = $this->getConnection();
            $stmt = $pdo->prepare($sql);

            if ($stmt === false) {
                throw new QueryException(
                    'Failed to prepare statement',
                    $sql,
                    $params
                );
            }

            $success = $stmt->execute($params);

            if (! $success) {
                $errorInfo = $stmt->errorInfo();

                throw new QueryException(
                    "Failed to execute statement: {$errorInfo[2]}",
                    $sql,
                    $params
                );
            }

            return $stmt;
        } catch (PDOException $e) {
            throw new QueryException(
                $e->getMessage(),
                $sql,
                $params,
                $e
            );
        }
    }
}
