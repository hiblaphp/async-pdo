<?php

declare(strict_types=1);

namespace Hibla\AsyncPDO\Utilities;

use Hibla\AsyncPDO\Exceptions\QueryException;
use PDO;
use PDOException;
use PDOStatement;
use Throwable;

/**
 * Handles query execution and result processing for PDO.
 * 
 * This class manages the complete lifecycle of PDO query execution including
 * query preparation, execution, and result processing with proper error handling.
 */
final class QueryExecutor
{
    /**
     * Executes a query with the specified result processing type.
     *
     * @param  PDO  $pdo  PDO connection
     * @param  string  $sql  SQL query/statement
     * @param  array<string|int, mixed>  $params  Query parameters
     * @param  string  $resultType  Type of result processing ('fetchAll', 'fetchOne', 'execute', 'fetchValue')
     * @return mixed Processed result based on result type
     *
     * @throws QueryException If query execution fails
     */
    public function executeQuery(
        PDO $pdo,
        string $sql,
        array $params,
        string $resultType
    ): mixed {
        try {
            $stmt = $pdo->prepare($sql);
            
            if ($stmt === false) {
                throw new QueryException(
                    'Failed to prepare statement',
                    $sql,
                    $params
                );
            }

            if (!$stmt->execute($params)) {
                throw new QueryException(
                    'Failed to execute statement',
                    $sql,
                    $params
                );
            }

            return $this->processResult($stmt, $resultType);
        } catch (QueryException $e) {
            throw $e;
        } catch (PDOException $e) {
            throw new QueryException(
                'Query execution failed: ' . $e->getMessage(),
                $sql,
                $params,
                $e
            );
        } catch (Throwable $e) {
            throw new QueryException(
                'Unexpected error during query execution: ' . $e->getMessage(),
                $sql,
                $params,
                $e
            );
        }
    }

    /**
     * Processes a query result based on the specified result type.
     *
     * @param  PDOStatement  $stmt  PDO statement
     * @param  string  $resultType  Type of result processing
     * @return mixed Processed result based on result type
     */
    private function processResult(PDOStatement $stmt, string $resultType): mixed
    {
        return match ($resultType) {
            'fetchAll' => $this->handleFetchAll($stmt),
            'fetchOne' => $this->handleFetchOne($stmt),
            'fetchValue' => $this->handleFetchValue($stmt),
            'execute' => $this->handleExecute($stmt),
            default => null,
        };
    }

    /**
     * Fetches all rows from a query result.
     *
     * @param  PDOStatement  $stmt  PDO statement
     * @return array<int, array<string, mixed>> Array of associative arrays
     */
    private function handleFetchAll(PDOStatement $stmt): array
    {
        /** @var array<int, array<string, mixed>> */
        $result = $stmt->fetchAll(PDO::FETCH_ASSOC);

        return $result;
    }

    /**
     * Fetches the first row from a query result.
     *
     * @param  PDOStatement  $stmt  PDO statement
     * @return array<string, mixed>|false Associative array or false if no rows
     */
    private function handleFetchOne(PDOStatement $stmt): array|false
    {
        /** @var array<string, mixed>|false */
        $result = $stmt->fetch(PDO::FETCH_ASSOC);

        return $result;
    }

    /**
     * Fetches a single column value from the first row.
     *
     * @param  PDOStatement  $stmt  PDO statement
     * @return mixed Scalar value or false if no rows
     */
    private function handleFetchValue(PDOStatement $stmt): mixed
    {
        return $stmt->fetch(PDO::FETCH_COLUMN);
    }

    /**
     * Gets the number of affected rows from a query result.
     *
     * @param  PDOStatement  $stmt  PDO statement
     * @return int Number of affected rows
     */
    private function handleExecute(PDOStatement $stmt): int
    {
        return $stmt->rowCount();
    }
}