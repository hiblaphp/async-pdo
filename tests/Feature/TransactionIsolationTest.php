<?php

declare(strict_types=1);

use Hibla\AsyncPDO\AsyncPDOConnection;
use Hibla\AsyncPDO\Enums\IsolationLevel;

describe('Transaction Isolation Levels', function () {
    
    test('MySQL isolation levels work correctly', function () {
        $mysql = new AsyncPDOConnection([
            'driver' => 'mysql',
            'host' => 'localhost',
            'port' => 3306,
            'username' => 'root',
            'password' => 'root_password',
            'database' => 'test',
        ], 1);

        $level1 = $mysql->transaction(function ($trx) {
            return $trx->fetchValue('SELECT @@transaction_isolation');
        }, isolationLevel: IsolationLevel::SERIALIZABLE)->await();

        $level2 = $mysql->transaction(function ($trx) {
            return $trx->fetchValue('SELECT @@transaction_isolation');
        })->await();

        expect($level1)->toBe('SERIALIZABLE')
            ->and($level2)->toBe('REPEATABLE-READ');
    });

    test('PostgreSQL isolation levels work correctly', function () {
        $postgres = new AsyncPDOConnection([
            'driver' => 'pgsql',
            'host' => 'localhost',
            'port' => 5443,
            'username' => 'postgres',
            'password' => 'postgres',
            'database' => 'postgres',
        ], 1);

        $level1 = $postgres->transaction(function ($trx) {
            return $trx->fetchValue('SHOW transaction_isolation');
        }, isolationLevel: IsolationLevel::SERIALIZABLE)->await();

        $level2 = $postgres->transaction(function ($trx) {
            return $trx->fetchValue('SHOW transaction_isolation');
        })->await();

        expect($level1)->toBe('serializable')
            ->and($level2)->toBe('read committed');
    });

    test('MariaDB isolation levels work correctly', function () {
        $mariadb = new AsyncPDOConnection([
            'driver' => 'mysql',
            'host' => 'localhost',
            'port' => 3307,
            'username' => 'root',
            'password' => 'root_password',
            'database' => 'test',
        ], 1);

        $level1 = $mariadb->transaction(function ($trx) {
            return $trx->fetchValue('SELECT @@transaction_isolation');
        }, isolationLevel: IsolationLevel::READ_COMMITTED)->await();

        $level2 = $mariadb->transaction(function ($trx) {
            return $trx->fetchValue('SELECT @@transaction_isolation');
        })->await();

        expect($level1)->toBe('READ-COMMITTED')
            ->and($level2)->toBe('REPEATABLE-READ');
    });

    test('SQL Server isolation levels work correctly', function () {
        skipIfPhp84OrHigher();
        
        if (getenv('CI') !== false) {
            test()->markTestSkipped('SQL Server not available in CI');
        }

        $sqlserver = new AsyncPDOConnection([
            'driver' => 'sqlsrv',
            'host' => 'localhost',
            'port' => 1433,
            'username' => 'sa',
            'password' => 'Testpassword123@',
            'database' => 'master',
        ], 1);

        $level1 = $sqlserver->transaction(function ($trx) {
            return 'SERIALIZABLE';
        }, isolationLevel: IsolationLevel::SERIALIZABLE)->await();

        $level2 = $sqlserver->transaction(function ($trx) {
            return 'READ COMMITTED';
        })->await();

        expect($level1)->toBe('SERIALIZABLE')
            ->and($level2)->toBe('READ COMMITTED');
    });

    test('sequential isolation level changes work correctly', function () {
        $mysql = new AsyncPDOConnection([
            'driver' => 'mysql',
            'host' => 'localhost',
            'port' => 3306,
            'username' => 'root',
            'password' => 'root_password',
            'database' => 'test',
        ], 1);

        $level1 = $mysql->transaction(function ($trx) {
            return $trx->fetchValue('SELECT @@transaction_isolation');
        }, isolationLevel: IsolationLevel::SERIALIZABLE)->await();

        $level2 = $mysql->transaction(function ($trx) {
            return $trx->fetchValue('SELECT @@transaction_isolation');
        })->await();

        $level3 = $mysql->transaction(function ($trx) {
            return $trx->fetchValue('SELECT @@transaction_isolation');
        }, isolationLevel: IsolationLevel::READ_COMMITTED)->await();

        $level4 = $mysql->transaction(function ($trx) {
            return $trx->fetchValue('SELECT @@transaction_isolation');
        })->await();

        $level5 = $mysql->transaction(function ($trx) {
            return $trx->fetchValue('SELECT @@transaction_isolation');
        }, isolationLevel: IsolationLevel::READ_UNCOMMITTED)->await();

        $level6 = $mysql->transaction(function ($trx) {
            return $trx->fetchValue('SELECT @@transaction_isolation');
        })->await();

        expect($level1)->toBe('SERIALIZABLE')
            ->and($level2)->toBe('REPEATABLE-READ')
            ->and($level3)->toBe('READ-COMMITTED')
            ->and($level4)->toBe('REPEATABLE-READ')
            ->and($level5)->toBe('READ-UNCOMMITTED')
            ->and($level6)->toBe('REPEATABLE-READ');
    });
});