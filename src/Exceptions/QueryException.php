<?php

declare(strict_types=1);

namespace Hibla\AsyncPDO\Exceptions;

use RuntimeException;
use Throwable;

/**
 * Exception thrown when a database query or statement execution fails.
 *
 * This exception provides context about the failed query including the SQL
 * statement, parameters used, and the underlying PDO error.
 */
final class QueryException extends RuntimeException
{
    /**
     * Creates a new QueryException.
     *
     * @param  string  $message  Exception message describing the failure
     * @param  string  $sql  The SQL query or statement that failed
     * @param  array<string|int, mixed>  $params  The parameters used in the query
     * @param  Throwable|null  $previous  Previous exception that caused this failure
     */
    public function __construct(
        string $message,
        private readonly string $sql = '',
        private readonly array $params = [],
        ?Throwable $previous = null
    ) {
        parent::__construct($message, 0, $previous);
    }

    /**
     * Gets the SQL query or statement that failed.
     *
     * @return string The SQL query
     */
    public function getSql(): string
    {
        return $this->sql;
    }

    /**
     * Gets the parameters used in the failed query.
     *
     * @return array<string|int, mixed> Query parameters
     */
    public function getParams(): array
    {
        return $this->params;
    }

    /**
     * Gets a formatted string representation of the exception.
     *
     * Includes the error message, SQL query, and parameters for debugging.
     *
     * @return string Formatted exception details
     */
    public function getDetails(): string
    {
        $details = $this->getMessage();

        if ($this->sql !== '') {
            $details .= "\nSQL: " . $this->sql;
        }

        if (count($this->params) > 0) {
            $details .= "\nParameters: " . json_encode($this->params, JSON_PRETTY_PRINT);
        }

        return $details;
    }
}
