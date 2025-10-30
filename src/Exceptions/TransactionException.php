<?php

declare(strict_types=1);

namespace Hibla\AsyncPDO\Exceptions;

use RuntimeException;

/**
 * Exception thrown when transaction operations fail or transaction state is corrupted.
 */
final class TransactionException extends RuntimeException
{
}