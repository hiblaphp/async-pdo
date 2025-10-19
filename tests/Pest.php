<?php

pest()->extend(Tests\TestCase::class)->in('Feature');
pest()->extend(Tests\TestCase::class)->in('Stress');
pest()->extend(Tests\TestCase::class)->in('Unit');
pest()->extend(Tests\TestCase::class)->in('Integration');

function skipIfPhp84OrHigher(): void
{
    if (version_compare(PHP_VERSION, '8.4.0', '>=')) {
        test()->markTestSkipped('SQL Server driver not available for PHP 8.4+');
    }
}
