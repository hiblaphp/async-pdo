<?php

declare(strict_types=1);

use Hibla\AsyncPDO\Utilities\DSNBuilder;

describe('DSNBuilder', function () {
    describe('MySQL', function () {
        it('builds MySQL DSN with all parameters', function () {
            $config = [
                'driver' => 'mysql',
                'host' => 'localhost',
                'port' => 3306,
                'database' => 'test_db',
                'charset' => 'utf8mb4',
            ];

            $dsn = DSNBuilder::build($config);

            expect($dsn)->toBe('mysql:host=localhost;port=3306;dbname=test_db;charset=utf8mb4');
        });

        it('builds MySQL DSN with custom port', function () {
            $config = [
                'driver' => 'mysql',
                'host' => 'db.example.com',
                'port' => 3307,
                'database' => 'my_database',
                'charset' => 'utf8',
            ];

            $dsn = DSNBuilder::build($config);

            expect($dsn)->toBe('mysql:host=db.example.com;port=3307;dbname=my_database;charset=utf8');
        });

        it('builds MySQL DSN with default port when port is 0', function () {
            $config = [
                'driver' => 'mysql',
                'host' => 'localhost',
                'port' => 0,
                'database' => 'test_db',
                'charset' => 'utf8mb4',
            ];

            $dsn = DSNBuilder::build($config);

            expect($dsn)->toBe('mysql:host=localhost;port=3306;dbname=test_db;charset=utf8mb4');
        });

        it('builds MySQL DSN without port parameter', function () {
            $config = [
                'driver' => 'mysql',
                'host' => 'localhost',
                'database' => 'test_db',
                'charset' => 'utf8mb4',
            ];

            $dsn = DSNBuilder::build($config);

            expect($dsn)->toBe('mysql:host=localhost;port=3306;dbname=test_db;charset=utf8mb4');
        });

        it('uses default charset when not specified', function () {
            $config = [
                'driver' => 'mysql',
                'host' => 'localhost',
                'database' => 'test_db',
            ];

            $dsn = DSNBuilder::build($config);

            expect($dsn)->toContain('charset=utf8mb4');
        });

        it('uses default host when not specified', function () {
            $config = [
                'driver' => 'mysql',
                'database' => 'test_db',
            ];

            $dsn = DSNBuilder::build($config);

            expect($dsn)->toContain('host=127.0.0.1');
        });
    });

    describe('PostgreSQL', function () {
        it('builds PostgreSQL DSN with all parameters', function () {
            $config = [
                'driver' => 'pgsql',
                'host' => 'localhost',
                'port' => 5432,
                'database' => 'test_db',
            ];

            $dsn = DSNBuilder::build($config);

            expect($dsn)->toBe('pgsql:host=localhost;port=5432;dbname=test_db');
        });

        it('builds PostgreSQL DSN using postgresql driver alias', function () {
            $config = [
                'driver' => 'postgresql',
                'host' => 'db.example.com',
                'port' => 5433,
                'database' => 'my_database',
            ];

            $dsn = DSNBuilder::build($config);

            expect($dsn)->toBe('pgsql:host=db.example.com;port=5433;dbname=my_database');
        });

        it('builds PostgreSQL DSN with default port', function () {
            $config = [
                'driver' => 'pgsql',
                'host' => 'localhost',
                'database' => 'test_db',
            ];

            $dsn = DSNBuilder::build($config);

            expect($dsn)->toBe('pgsql:host=localhost;port=5432;dbname=test_db');
        });
    });

    describe('SQLite', function () {
        it('builds SQLite DSN with file path', function () {
            $config = [
                'driver' => 'sqlite',
                'database' => '/path/to/database.db',
            ];

            $dsn = DSNBuilder::build($config);

            expect($dsn)->toBe('sqlite:/path/to/database.db');
        });

        it('builds SQLite DSN with memory database', function () {
            $config = [
                'driver' => 'sqlite',
                'database' => ':memory:',
            ];

            $dsn = DSNBuilder::build($config);

            expect($dsn)->toBe('sqlite::memory:');
        });

        it('builds SQLite DSN with relative path', function () {
            $config = [
                'driver' => 'sqlite',
                'database' => 'database.db',
            ];

            $dsn = DSNBuilder::build($config);

            expect($dsn)->toBe('sqlite:database.db');
        });
    });

    describe('SQL Server', function () {
        it('builds SQL Server DSN with all parameters', function () {
            $config = [
                'driver' => 'sqlsrv',
                'host' => 'localhost',
                'port' => 1433,
                'database' => 'test_db',
            ];

            $dsn = DSNBuilder::build($config);

            expect($dsn)->toBe('sqlsrv:server=localhost;Database=test_db');
        });

        it('builds SQL Server DSN with custom port', function () {
            $config = [
                'driver' => 'sqlsrv',
                'host' => 'db.example.com',
                'port' => 1434,
                'database' => 'my_database',
            ];

            $dsn = DSNBuilder::build($config);

            expect($dsn)->toBe('sqlsrv:server=db.example.com,1434;Database=my_database');
        });

        it('builds SQL Server DSN using mssql driver alias', function () {
            $config = [
                'driver' => 'mssql',
                'host' => 'localhost',
                'database' => 'test_db',
            ];

            $dsn = DSNBuilder::build($config);

            expect($dsn)->toBe('sqlsrv:server=localhost;Database=test_db');
        });

        it('builds SQL Server DSN without database', function () {
            $config = [
                'driver' => 'sqlsrv',
                'host' => 'localhost',
            ];

            $dsn = DSNBuilder::build($config);

            expect($dsn)->toBe('sqlsrv:server=localhost');
        });
    });

    describe('Oracle', function () {
        it('builds Oracle DSN with all parameters', function () {
            $config = [
                'driver' => 'oci',
                'host' => 'localhost',
                'port' => 1521,
                'database' => 'ORCL',
                'charset' => 'AL32UTF8',
            ];

            $dsn = DSNBuilder::build($config);

            expect($dsn)->toBe('oci:dbname=//localhost:1521/ORCL;charset=AL32UTF8');
        });

        it('builds Oracle DSN using oracle driver alias', function () {
            $config = [
                'driver' => 'oracle',
                'host' => 'db.example.com',
                'port' => 1521,
                'database' => 'XE',
            ];

            $dsn = DSNBuilder::build($config);

            expect($dsn)->toBe('oci:dbname=//db.example.com:1521/XE');
        });

        it('builds Oracle DSN without host', function () {
            $config = [
                'driver' => 'oci',
                'database' => 'ORCL',
            ];

            $dsn = DSNBuilder::build($config);

            expect($dsn)->toBe('oci:dbname=ORCL');
        });

        it('builds Oracle DSN without port', function () {
            $config = [
                'driver' => 'oci',
                'host' => 'localhost',
                'database' => 'ORCL',
            ];

            $dsn = DSNBuilder::build($config);

            expect($dsn)->toBe('oci:dbname=//localhost/ORCL');
        });

        it('builds Oracle DSN without charset', function () {
            $config = [
                'driver' => 'oci',
                'host' => 'localhost',
                'port' => 1521,
                'database' => 'ORCL',
            ];

            $dsn = DSNBuilder::build($config);

            expect($dsn)->toBe('oci:dbname=//localhost:1521/ORCL');
        });
    });

    describe('IBM DB2', function () {
        it('builds IBM DB2 DSN', function () {
            $config = [
                'driver' => 'ibm',
                'dsn' => 'SAMPLE',
            ];

            $dsn = DSNBuilder::build($config);

            expect($dsn)->toBe('ibm:SAMPLE');
        });

        it('builds IBM DB2 DSN using database field', function () {
            $config = [
                'driver' => 'ibm',
                'database' => 'TESTDB',
            ];

            $dsn = DSNBuilder::build($config);

            expect($dsn)->toBe('ibm:TESTDB');
        });

        it('builds IBM DB2 DSN using db2 driver alias', function () {
            $config = [
                'driver' => 'db2',
                'database' => 'SAMPLE',
            ];

            $dsn = DSNBuilder::build($config);

            expect($dsn)->toBe('ibm:SAMPLE');
        });
    });

    describe('ODBC', function () {
        it('builds ODBC DSN', function () {
            $config = [
                'driver' => 'odbc',
                'dsn' => 'Driver={SQL Server};Server=localhost;Database=test_db',
            ];

            $dsn = DSNBuilder::build($config);

            expect($dsn)->toBe('odbc:Driver={SQL Server};Server=localhost;Database=test_db');
        });

        it('builds ODBC DSN using database field', function () {
            $config = [
                'driver' => 'odbc',
                'database' => 'MyDSN',
            ];

            $dsn = DSNBuilder::build($config);

            expect($dsn)->toBe('odbc:MyDSN');
        });
    });

    describe('Firebird', function () {
        it('builds Firebird DSN', function () {
            $config = [
                'driver' => 'firebird',
                'database' => '/path/to/database.fdb',
            ];

            $dsn = DSNBuilder::build($config);

            expect($dsn)->toBe('firebird:dbname=/path/to/database.fdb');
        });

        it('builds Firebird DSN with remote server', function () {
            $config = [
                'driver' => 'firebird',
                'database' => 'localhost:/path/to/database.fdb',
            ];

            $dsn = DSNBuilder::build($config);

            expect($dsn)->toBe('firebird:dbname=localhost:/path/to/database.fdb');
        });
    });

    describe('Informix', function () {
        it('builds Informix DSN with all parameters', function () {
            $config = [
                'driver' => 'informix',
                'host' => 'localhost',
                'database' => 'test_db',
                'server' => 'ol_informix1170',
                'protocol' => 'onsoctcp',
                'service' => '9800',
            ];

            $dsn = DSNBuilder::build($config);

            expect($dsn)->toBe('informix:host=localhost;database=test_db;server=ol_informix1170;protocol=onsoctcp;service=9800');
        });

        it('builds Informix DSN with partial parameters', function () {
            $config = [
                'driver' => 'informix',
                'host' => 'localhost',
                'database' => 'test_db',
                'server' => 'ol_informix1170',
            ];

            $dsn = DSNBuilder::build($config);

            expect($dsn)->toBe('informix:host=localhost;database=test_db;server=ol_informix1170');
        });

        it('builds Informix DSN with only database', function () {
            $config = [
                'driver' => 'informix',
                'database' => 'test_db',
            ];

            $dsn = DSNBuilder::build($config);

            expect($dsn)->toBe('informix:database=test_db');
        });

        it('ignores empty string values in Informix DSN', function () {
            $config = [
                'driver' => 'informix',
                'host' => 'localhost',
                'database' => 'test_db',
                'server' => '',
                'protocol' => 'onsoctcp',
            ];

            $dsn = DSNBuilder::build($config);

            expect($dsn)->toBe('informix:host=localhost;database=test_db;protocol=onsoctcp');
        });
    });

    describe('Unsupported Drivers', function () {
        it('throws exception for unsupported driver', function () {
            $config = [
                'driver' => 'unsupported_driver',
                'database' => 'test_db',
            ];

            expect(fn () => DSNBuilder::build($config))
                ->toThrow(PDOException::class, 'Unsupported database driver: unsupported_driver')
            ;
        });

        it('throws exception for unknown driver', function () {
            $config = [
                'driver' => 'mongodb',
                'database' => 'test_db',
            ];

            expect(fn () => DSNBuilder::build($config))
                ->toThrow(PDOException::class, 'Unsupported database driver: mongodb')
            ;
        });
    });

    describe('Case Insensitivity', function () {
        it('handles uppercase driver names', function () {
            $config = [
                'driver' => 'MYSQL',
                'host' => 'localhost',
                'database' => 'test_db',
            ];

            $dsn = DSNBuilder::build($config);

            expect($dsn)->toContain('mysql:');
        });

        it('handles mixed case driver names', function () {
            $config = [
                'driver' => 'PostgreSQL',
                'host' => 'localhost',
                'database' => 'test_db',
            ];

            $dsn = DSNBuilder::build($config);

            expect($dsn)->toContain('pgsql:');
        });
    });

    describe('Individual Builder Methods', function () {
        it('buildMySQL creates correct DSN', function () {
            $dsn = DSNBuilder::buildMySQL('localhost', 3306, 'test_db', 'utf8');

            expect($dsn)->toBe('mysql:host=localhost;port=3306;dbname=test_db;charset=utf8');
        });

        it('buildPostgreSQL creates correct DSN', function () {
            $dsn = DSNBuilder::buildPostgreSQL('localhost', 5432, 'test_db');

            expect($dsn)->toBe('pgsql:host=localhost;port=5432;dbname=test_db');
        });

        it('buildSQLite creates correct DSN', function () {
            $dsn = DSNBuilder::buildSQLite('/path/to/db.sqlite');

            expect($dsn)->toBe('sqlite:/path/to/db.sqlite');
        });

        it('buildSqlServer creates correct DSN with default port', function () {
            $config = [
                'host' => 'localhost',
                'database' => 'test_db',
                'port' => 1433,
            ];

            $dsn = DSNBuilder::buildSqlServer($config);

            expect($dsn)->toBe('sqlsrv:server=localhost;Database=test_db');
        });

        it('buildSqlServer creates correct DSN with custom port', function () {
            $config = [
                'host' => 'localhost',
                'database' => 'test_db',
                'port' => 1434,
            ];

            $dsn = DSNBuilder::buildSqlServer($config);

            expect($dsn)->toBe('sqlsrv:server=localhost,1434;Database=test_db');
        });

        it('buildOracle creates correct DSN with host and port', function () {
            $config = [
                'host' => 'localhost',
                'port' => 1521,
                'database' => 'ORCL',
                'charset' => 'AL32UTF8',
            ];

            $dsn = DSNBuilder::buildOracle($config);

            expect($dsn)->toBe('oci:dbname=//localhost:1521/ORCL;charset=AL32UTF8');
        });

        it('buildIBM creates correct DSN', function () {
            $dsn = DSNBuilder::buildIBM('SAMPLE');

            expect($dsn)->toBe('ibm:SAMPLE');
        });

        it('buildODBC creates correct DSN', function () {
            $dsn = DSNBuilder::buildODBC('MyDSN');

            expect($dsn)->toBe('odbc:MyDSN');
        });

        it('buildFirebird creates correct DSN', function () {
            $dsn = DSNBuilder::buildFirebird('/path/to/database.fdb');

            expect($dsn)->toBe('firebird:dbname=/path/to/database.fdb');
        });

        it('buildInformix creates correct DSN', function () {
            $config = [
                'host' => 'localhost',
                'database' => 'test_db',
                'server' => 'ol_informix1170',
            ];

            $dsn = DSNBuilder::buildInformix($config);

            expect($dsn)->toBe('informix:host=localhost;database=test_db;server=ol_informix1170');
        });
    });

    describe('Edge Cases', function () {
        it('handles empty database name gracefully', function () {
            $config = [
                'driver' => 'mysql',
                'host' => 'localhost',
                'database' => '',
            ];

            $dsn = DSNBuilder::build($config);

            expect($dsn)->toContain('dbname=');
        });

        it('handles null values in config', function () {
            $config = [
                'driver' => 'mysql',
                'host' => null,
                'database' => 'test_db',
            ];

            $dsn = DSNBuilder::build($config);

            expect($dsn)->toContain('host=127.0.0.1'); // Default host
        });

        it('handles numeric strings for port', function () {
            $config = [
                'driver' => 'mysql',
                'host' => 'localhost',
                'port' => '3307',
                'database' => 'test_db',
            ];

            $dsn = DSNBuilder::build($config);

            expect($dsn)->toContain('port=3307');
        });
    });
});
