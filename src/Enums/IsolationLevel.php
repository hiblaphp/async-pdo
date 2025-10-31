<?php

declare(strict_types=1);

namespace Hibla\AsyncPDO\Enums;

/**
 * Database transaction isolation levels.
 * 
 * Defines the standard SQL isolation levels for transaction control.
 */
enum IsolationLevel: string
{
    case READ_UNCOMMITTED = 'READ UNCOMMITTED';
    case READ_COMMITTED = 'READ COMMITTED';
    case REPEATABLE_READ = 'REPEATABLE READ';
    case SERIALIZABLE = 'SERIALIZABLE';
}