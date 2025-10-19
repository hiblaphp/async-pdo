<?php

namespace Hibla\AsyncPDO\Exception;

/**
 * Thrown when AsyncPDO operations are attempted before initialization
 */
class AsyncPDONotInitializedException extends \RuntimeException
{
    public function __construct()
    {
        parent::__construct(
            'AsyncPDO has not been initialized. Please call AsyncPDO::init() at application startup.'
        );
    }
}
