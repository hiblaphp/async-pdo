<?php

use Hibla\AsyncPDO\Utilities\DatabaseConfigFactory;

test('mysql creates correct configuration with defaults', function () {
    $config = DatabaseConfigFactory::mysql();

    expect($config['driver'])->toBe('mysql')
        ->and($config['host'])->toBe('localhost')
        ->and($config['port'])->toBe(3306)
        ->and($config['database'])->toBe('test')
        ->and($config['username'])->toBe('root')
        ->and($config['password'])->toBe('')
        ->and($config['charset'])->toBe('utf8mb4')
        ->and($config['options'])->toHaveKey(PDO::ATTR_ERRMODE)
        ->and($config['options'][PDO::ATTR_ERRMODE])->toBe(PDO::ERRMODE_EXCEPTION)
    ;
});

test('mysql merges custom configuration', function () {
    $config = DatabaseConfigFactory::mysql([
        'host' => 'custom-host',
        'port' => 3307,
        'database' => 'custom_db',
        'username' => 'custom_user',
        'password' => 'custom_pass',
    ]);

    expect($config['host'])->toBe('custom-host')
        ->and($config['port'])->toBe(3307)
        ->and($config['database'])->toBe('custom_db')
        ->and($config['username'])->toBe('custom_user')
        ->and($config['password'])->toBe('custom_pass')
        ->and($config['driver'])->toBe('mysql')
    ;
});

test('postgresql creates correct configuration with defaults', function () {
    $config = DatabaseConfigFactory::postgresql();

    expect($config['driver'])->toBe('pgsql')
        ->and($config['host'])->toBe('localhost')
        ->and($config['port'])->toBe(5432)
        ->and($config['database'])->toBe('test')
        ->and($config['username'])->toBe('postgres')
        ->and($config['password'])->toBe('')
        ->and($config['charset'])->toBe('utf8')
    ;
});

test('postgresql merges custom configuration', function () {
    $config = DatabaseConfigFactory::postgresql([
        'host' => 'pg-host',
        'database' => 'mydb',
    ]);

    expect($config['host'])->toBe('pg-host')
        ->and($config['database'])->toBe('mydb')
        ->and($config['driver'])->toBe('pgsql')
    ;
});

test('sqlite creates correct configuration', function () {
    $config = DatabaseConfigFactory::sqlite(':memory:');

    expect($config['driver'])->toBe('sqlite')
        ->and($config['database'])->toBe(':memory:')
        ->and($config['username'])->toBe('')
        ->and($config['password'])->toBe('')
    ;
});

test('sqlite uses default in-memory database', function () {
    $config = DatabaseConfigFactory::sqlite();

    expect($config['database'])->toBe('file::memory:?cache=shared');
});

test('sqlserver creates correct configuration with defaults', function () {
    $config = DatabaseConfigFactory::sqlserver();

    expect($config['driver'])->toBe('sqlsrv')
        ->and($config['host'])->toBe('localhost')
        ->and($config['port'])->toBe(1433)
        ->and($config['database'])->toBe('test')
    ;
});

test('oracle creates correct configuration with defaults', function () {
    $config = DatabaseConfigFactory::oracle();

    expect($config['driver'])->toBe('oci')
        ->and($config['host'])->toBe('localhost')
        ->and($config['port'])->toBe(1521)
        ->and($config['database'])->toBe('xe')
        ->and($config['charset'])->toBe('AL32UTF8')
    ;
});

test('ibm creates correct configuration with defaults', function () {
    $config = DatabaseConfigFactory::ibm();

    expect($config['driver'])->toBe('ibm')
        ->and($config['host'])->toBe('localhost')
        ->and($config['port'])->toBe(50000)
        ->and($config['database'])->toBe('test')
    ;
});

test('firebird creates correct configuration with defaults', function () {
    $config = DatabaseConfigFactory::firebird();

    expect($config['driver'])->toBe('firebird')
        ->and($config['host'])->toBe('localhost')
        ->and($config['port'])->toBe(3050)
        ->and($config['username'])->toBe('SYSDBA')
        ->and($config['password'])->toBe('masterkey')
        ->and($config['charset'])->toBe('UTF8')
    ;
});

test('odbc creates correct configuration with defaults', function () {
    $config = DatabaseConfigFactory::odbc();

    expect($config['driver'])->toBe('odbc')
        ->and($config['dsn'])->toBe('MyDataSource')
    ;
});

test('informix creates correct configuration with defaults', function () {
    $config = DatabaseConfigFactory::informix();

    expect($config['driver'])->toBe('informix')
        ->and($config['host'])->toBe('localhost')
        ->and($config['database'])->toBe('sysmaster')
        ->and($config['server'])->toBe('ol_informix')
        ->and($config['protocol'])->toBe('onsoctcp')
    ;
});

test('fromUrl parses mysql url correctly', function () {
    $url = 'mysql://user:pass@localhost:3306/mydb';
    $config = DatabaseConfigFactory::fromUrl($url);

    expect($config['driver'])->toBe('mysql')
        ->and($config['host'])->toBe('localhost')
        ->and($config['port'])->toBe(3306)
        ->and($config['database'])->toBe('mydb')
        ->and($config['username'])->toBe('user')
        ->and($config['password'])->toBe('pass')
    ;
});

test('fromUrl parses postgresql url correctly', function () {
    $url = 'pgsql://pguser:pgpass@db.example.com:5432/production';
    $config = DatabaseConfigFactory::fromUrl($url);

    expect($config['driver'])->toBe('pgsql')
        ->and($config['host'])->toBe('db.example.com')
        ->and($config['port'])->toBe(5432)
        ->and($config['database'])->toBe('production')
        ->and($config['username'])->toBe('pguser')
        ->and($config['password'])->toBe('pgpass')
    ;
});

test('fromUrl handles url without port', function () {
    $url = 'mysql://user:pass@localhost/mydb';
    $config = DatabaseConfigFactory::fromUrl($url);

    expect($config['port'])->toBeNull()
        ->and($config['host'])->toBe('localhost')
    ;
});

test('fromUrl handles url without credentials', function () {
    $url = 'mysql://localhost/mydb';
    $config = DatabaseConfigFactory::fromUrl($url);

    expect($config['username'])->toBe('')
        ->and($config['password'])->toBe('')
        ->and($config['database'])->toBe('mydb')
        ->and($config['driver'])->toBe('mysql')
    ;
});

test('fromUrl parses pdo options from query string', function () {
    $url = 'mysql://user:pass@localhost/mydb?pdo_attr_errmode=2&pdo_attr_timeout=30';
    $config = DatabaseConfigFactory::fromUrl($url);

    expect($config['options'])->toHaveKey(PDO::ATTR_ERRMODE)
        ->and($config['options'][PDO::ATTR_ERRMODE])->toBe('2')
        ->and($config['options'])->toHaveKey(PDO::ATTR_TIMEOUT)
        ->and($config['options'][PDO::ATTR_TIMEOUT])->toBe('30')
    ;
});

test('fromUrl ignores non-pdo query parameters', function () {
    $url = 'mysql://user:pass@localhost/mydb?charset=utf8&pdo_attr_errmode=2';
    $config = DatabaseConfigFactory::fromUrl($url);

    expect($config['options'])->toHaveKey(PDO::ATTR_ERRMODE)
        ->and($config['options'])->not->toHaveKey('charset')
    ;
});

test('fromUrl uses defaults for missing components', function () {
    $url = 'mydb';
    $config = DatabaseConfigFactory::fromUrl($url);

    expect($config['driver'])->toBe('mysql')
        ->and($config['host'])->toBe('localhost')
        ->and($config['database'])->toBe('mydb')
    ;
});

test('fromUrl throws exception for invalid url', function () {
    DatabaseConfigFactory::fromUrl('http:///invalid url with spaces');
})->throws(InvalidArgumentException::class, 'Invalid database URL provided.');

test('all drivers include required PDO options', function () {
    $configs = [
        DatabaseConfigFactory::mysql(),
        DatabaseConfigFactory::postgresql(),
        DatabaseConfigFactory::sqlite(),
        DatabaseConfigFactory::sqlserver(),
        DatabaseConfigFactory::oracle(),
        DatabaseConfigFactory::ibm(),
        DatabaseConfigFactory::firebird(),
        DatabaseConfigFactory::odbc(),
        DatabaseConfigFactory::informix(),
    ];

    foreach ($configs as $config) {
        expect($config['options'])->toHaveKey(PDO::ATTR_ERRMODE)
            ->and($config['options'][PDO::ATTR_ERRMODE])->toBe(PDO::ERRMODE_EXCEPTION)
            ->and($config['options'])->toHaveKey(PDO::ATTR_DEFAULT_FETCH_MODE)
            ->and($config['options'][PDO::ATTR_DEFAULT_FETCH_MODE])->toBe(PDO::FETCH_ASSOC)
        ;
    }
});

test('fromUrl handles special characters in password', function () {
    $url = 'mysql://user:p%40ss%40word@localhost/mydb';
    $config = DatabaseConfigFactory::fromUrl($url);

    expect($config['password'])->toBe('p@ss@word');
});

test('fromUrl handles empty database name', function () {
    $url = 'mysql://user:pass@localhost';
    $config = DatabaseConfigFactory::fromUrl($url);

    expect($config['database'])->toBe('');
});
