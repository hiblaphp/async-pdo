<?php

declare(strict_types=1);

namespace Hibla\AsyncPDO\Exceptions;

use RuntimeException;

/**
 * Exception thrown when transaction-specific operations are called outside a transaction context.
 */
final class NotInTransactionException extends RuntimeException
{
}
