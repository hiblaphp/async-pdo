<?php

use Hibla\AsyncPDO\Manager\PoolManager;

describe('Database Drivers Integration', function () {
    describe('SQLite', function () {
        it('connects and executes query', function () {
            $config = [
                'driver' => 'sqlite',
                'database' => ':memory:',
            ];

            $pool = new PoolManager($config, 2);
            $connection = $pool->get()->await();

            expect($connection)->toBeInstanceOf(PDO::class);

            $connection->exec('CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)');
            $connection->exec("INSERT INTO test (name) VALUES ('SQLite Test')");
            $result = $connection->query('SELECT name FROM test')->fetch(PDO::FETCH_ASSOC);

            expect($result['name'])->toBe('SQLite Test');

            $pool->release($connection);
            $pool->close();
        });
    });

    describe('MySQL', function () {
        it('connects and executes query', function () {
            if (empty($_ENV['MYSQL_HOST'])) {
                test()->markTestSkipped('MySQL not configured');
            }

            $config = [
                'driver' => 'mysql',
                'host' => $_ENV['MYSQL_HOST'] ?? 'localhost',
                'port' => (int) ($_ENV['MYSQL_PORT'] ?? 3306),
                'database' => $_ENV['MYSQL_DATABASE'] ?? 'test',
                'username' => $_ENV['MYSQL_USERNAME'] ?? 'root',
                'password' => $_ENV['MYSQL_PASSWORD'] ?? '',
                'charset' => 'utf8mb4',
            ];

            $pool = new PoolManager($config, 2);
            $connection = $pool->get()->await();

            expect($connection)->toBeInstanceOf(PDO::class);

            $connection->exec('CREATE TABLE IF NOT EXISTS mysql_test (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(255))');
            $connection->exec("INSERT INTO mysql_test (name) VALUES ('MySQL Test')");
            $result = $connection->query('SELECT name FROM mysql_test ORDER BY id DESC LIMIT 1')->fetch(PDO::FETCH_ASSOC);

            expect($result['name'])->toBe('MySQL Test');

            $connection->exec('DROP TABLE IF EXISTS mysql_test');

            $pool->release($connection);
            $pool->close();
        });

        it('handles connection pooling correctly', function () {
            if (empty($_ENV['MYSQL_HOST'])) {
                test()->markTestSkipped('MySQL not configured');
            }

            $config = [
                'driver' => 'mysql',
                'host' => $_ENV['MYSQL_HOST'] ?? 'localhost',
                'port' => (int) ($_ENV['MYSQL_PORT'] ?? 3306),
                'database' => $_ENV['MYSQL_DATABASE'] ?? 'test',
                'username' => $_ENV['MYSQL_USERNAME'] ?? 'root',
                'password' => $_ENV['MYSQL_PASSWORD'] ?? '',
            ];

            $pool = new PoolManager($config, 3);

            $conn1 = $pool->get()->await();
            $conn2 = $pool->get()->await();
            $conn3 = $pool->get()->await();

            expect($pool->getStats()['active_connections'])->toBe(3);

            $pool->release($conn1);
            expect($pool->getStats()['pooled_connections'])->toBe(1);

            $conn4 = $pool->get()->await();
            expect($conn4)->toBe($conn1);

            $pool->release($conn2);
            $pool->release($conn3);
            $pool->release($conn4);
            $pool->close();
        });
    });

    describe('PostgreSQL', function () {
        it('connects and executes query', function () {
            if (empty($_ENV['PGSQL_HOST'])) {
                test()->markTestSkipped('PostgreSQL not configured');
            }

            $config = [
                'driver' => 'pgsql',
                'host' => $_ENV['PGSQL_HOST'] ?? 'localhost',
                'port' => (int) ($_ENV['PGSQL_PORT'] ?? 5432),
                'database' => $_ENV['PGSQL_DATABASE'] ?? 'test',
                'username' => $_ENV['PGSQL_USERNAME'] ?? 'postgres',
                'password' => $_ENV['PGSQL_PASSWORD'] ?? 'postgres',
            ];

            $pool = new PoolManager($config, 2);
            $connection = $pool->get()->await();

            expect($connection)->toBeInstanceOf(PDO::class);

            $connection->exec('CREATE TABLE IF NOT EXISTS pgsql_test (id SERIAL PRIMARY KEY, name VARCHAR(255))');
            $connection->exec("INSERT INTO pgsql_test (name) VALUES ('PostgreSQL Test')");
            $result = $connection->query('SELECT name FROM pgsql_test ORDER BY id DESC LIMIT 1')->fetch(PDO::FETCH_ASSOC);

            expect($result['name'])->toBe('PostgreSQL Test');

            $connection->exec('DROP TABLE IF EXISTS pgsql_test');

            $pool->release($connection);
            $pool->close();
        });

        it('handles connection pooling correctly', function () {
            if (empty($_ENV['PGSQL_HOST'])) {
                test()->markTestSkipped('PostgreSQL not configured');
            }

            $config = [
                'driver' => 'pgsql',
                'host' => $_ENV['PGSQL_HOST'] ?? 'localhost',
                'port' => (int) ($_ENV['PGSQL_PORT'] ?? 5432),
                'database' => $_ENV['PGSQL_DATABASE'] ?? 'test',
                'username' => $_ENV['PGSQL_USERNAME'] ?? 'postgres',
                'password' => $_ENV['PGSQL_PASSWORD'] ?? 'postgres',
            ];

            $pool = new PoolManager($config, 3);

            $conn1 = $pool->get()->await();
            $conn2 = $pool->get()->await();

            expect($pool->getStats()['active_connections'])->toBe(2);

            $pool->release($conn1);
            expect($pool->getStats()['pooled_connections'])->toBe(1);

            $conn3 = $pool->get()->await();
            expect($conn3)->toBe($conn1);

            $pool->release($conn2);
            $pool->release($conn3);
            $pool->close();
        });
    });

    describe('SQL Server', function () {
        it('connects and executes query', function () {
            if (getenv('CI')) {
                test()->markTestSkipped('SQL Server tests skipped in CI environment');
            }

            skipIfPhp84OrHigher();

            if (empty($_ENV['SQLSRV_HOST'])) {
                test()->markTestSkipped('SQL Server not configured');
            }

            $config = [
                'driver' => 'sqlsrv',
                'host' => $_ENV['SQLSRV_HOST'] ?? 'localhost',
                'port' => (int) ($_ENV['SQLSRV_PORT'] ?? 1433),
                'database' => $_ENV['SQLSRV_DATABASE'] ?? 'master',
                'username' => $_ENV['SQLSRV_USERNAME'] ?? 'sa',
                'password' => $_ENV['SQLSRV_PASSWORD'] ?? 'YourStrong@Passw0rd',
            ];

            $pool = new PoolManager($config, 2);
            $connection = $pool->get()->await();

            expect($connection)->toBeInstanceOf(PDO::class);

            $connection->exec('IF OBJECT_ID(\'dbo.sqlsrv_test\', \'U\') IS NOT NULL DROP TABLE dbo.sqlsrv_test');
            $connection->exec('CREATE TABLE sqlsrv_test (id INT IDENTITY(1,1) PRIMARY KEY, name NVARCHAR(255))');
            $connection->exec("INSERT INTO sqlsrv_test (name) VALUES ('SQL Server Test')");
            $result = $connection->query('SELECT TOP 1 name FROM sqlsrv_test ORDER BY id DESC')->fetch(PDO::FETCH_ASSOC);

            expect($result['name'])->toBe('SQL Server Test');

            $connection->exec('DROP TABLE IF EXISTS sqlsrv_test');

            $pool->release($connection);
            $pool->close();
        });
    });

    describe('MariaDB', function () {
        it('connects and executes query', function () {
            if (empty($_ENV['MARIADB_HOST'])) {
                test()->markTestSkipped('MariaDB not configured');
            }

            $config = [
                'driver' => 'mysql', // MariaDB uses mysql driver
                'host' => $_ENV['MARIADB_HOST'] ?? 'localhost',
                'port' => (int) ($_ENV['MARIADB_PORT'] ?? 3306),
                'database' => $_ENV['MARIADB_DATABASE'] ?? 'test',
                'username' => $_ENV['MARIADB_USERNAME'] ?? 'root',
                'password' => $_ENV['MARIADB_PASSWORD'] ?? '',
                'charset' => 'utf8mb4',
            ];

            $pool = new PoolManager($config, 2);
            $connection = $pool->get()->await();

            expect($connection)->toBeInstanceOf(PDO::class);

            $connection->exec('CREATE TABLE IF NOT EXISTS mariadb_test (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(255))');
            $connection->exec("INSERT INTO mariadb_test (name) VALUES ('MariaDB Test')");
            $result = $connection->query('SELECT name FROM mariadb_test ORDER BY id DESC LIMIT 1')->fetch(PDO::FETCH_ASSOC);

            expect($result['name'])->toBe('MariaDB Test');

            $connection->exec('DROP TABLE IF EXISTS mariadb_test');

            $pool->release($connection);
            $pool->close();
        });
    });
});
