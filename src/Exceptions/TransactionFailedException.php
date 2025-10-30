<?php

declare(strict_types=1);

namespace Hibla\AsyncPDO\Exceptions;

use RuntimeException;
use Throwable;

/**
 * Exception thrown when a transaction fails after all retry attempts.
 */
final class TransactionFailedException extends RuntimeException
{
    /**
     * Creates a new TransactionFailedException.
     *
     * @param  string  $message  Exception message
     * @param  int  $attempts  Number of attempts made
     * @param  Throwable|null  $previous  Previous exception that caused the failure
     */
    public function __construct(
        string $message,
        private readonly int $attempts,
        ?Throwable $previous = null
    ) {
        parent::__construct($message, 0, $previous);
    }

    /**
     * Gets the number of attempts made before failure.
     *
     * @return int Number of attempts
     */
    public function getAttempts(): int
    {
        return $this->attempts;
    }
}