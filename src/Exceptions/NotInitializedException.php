<?php

declare(strict_types=1);

namespace Hibla\AsyncPDO\Exceptions;

use RuntimeException;

/**
 * Exception thrown when operations are attempted on an uninitialized or reset connection.
 */
final class NotInitializedException extends RuntimeException
{
}