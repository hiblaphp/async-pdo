<?php

namespace Hibla\AsyncPDO\Utilities;

use PDOException;

class DSNBuilder
{
    /**
     * Builds a Data Source Name (DSN) string for PDO from a configuration array.
     *
     * @param  array<string, mixed>  $config  The database configuration.
     * @return string The formatted DSN string.
     *
     * @throws PDOException If the driver is not supported.
     */
    public static function build(array $config): string
    {
        /** @var string $driver */
        $driver = $config['driver'];

        $host = is_scalar($config['host'] ?? null) ? (string) $config['host'] : '127.0.0.1';
        $port = is_numeric($config['port'] ?? null) ? (int) $config['port'] : 0;
        $database = is_scalar($config['database'] ?? null) ? (string) $config['database'] : '';
        $charset = is_scalar($config['charset'] ?? null) ? (string) $config['charset'] : 'utf8mb4';
        $dsnVal = is_scalar($config['dsn'] ?? null) ? (string) $config['dsn'] : $database;

        return match (strtolower($driver)) {
            'mysql' => self::buildMySQL($host, $port, $database, $charset),
            'pgsql', 'postgresql' => self::buildPostgreSQL($host, $port, $database),
            'sqlite' => self::buildSQLite($database),
            'sqlsrv', 'mssql' => self::buildSqlServer($config),
            'oci', 'oracle' => self::buildOracle($config),
            'ibm', 'db2' => self::buildIBM($dsnVal),
            'odbc' => self::buildODBC($dsnVal),
            'firebird' => self::buildFirebird($database),
            'informix' => self::buildInformix($config),
            default => throw new PDOException("Unsupported database driver: {$driver}")
        };
    }

    /**
     * Builds a MySQL DSN string.
     *
     * @param  string  $host
     * @param  int  $port
     * @param  string  $database
     * @param  string  $charset
     * @return string
     */
    public static function buildMySQL(string $host, int $port, string $database, string $charset = 'utf8mb4'): string
    {
        return sprintf(
            'mysql:host=%s;port=%d;dbname=%s;charset=%s',
            $host,
            $port > 0 ? $port : 3306,
            $database,
            $charset
        );
    }

    /**
     * Builds a PostgreSQL DSN string.
     *
     * @param  string  $host
     * @param  int  $port
     * @param  string  $database
     * @return string
     */
    public static function buildPostgreSQL(string $host, int $port, string $database): string
    {
        return sprintf(
            'pgsql:host=%s;port=%d;dbname=%s',
            $host,
            $port > 0 ? $port : 5432,
            $database
        );
    }

    /**
     * Builds a SQLite DSN string.
     *
     * @param  string  $database
     * @return string
     */
    public static function buildSQLite(string $database): string
    {
        return 'sqlite:' . $database;
    }

    /**
     * Builds a SQL Server DSN string from a configuration array.
     *
     * @param  array<string, mixed>  $config
     * @return string
     */
    public static function buildSqlServer(array $config): string
    {
        $host = is_scalar($config['host'] ?? null) ? (string) $config['host'] : '';
        $database = is_scalar($config['database'] ?? null) ? (string) $config['database'] : '';
        $port = is_numeric($config['port'] ?? null) ? (int) $config['port'] : 1433;

        $dsn = 'sqlsrv:server=' . $host;
        if ($port !== 1433) {
            $dsn .= ',' . $port;
        }
        if ($database !== '') {
            $dsn .= ';Database=' . $database;
        }

        return $dsn;
    }

    /**
     * Builds an Oracle DSN string from a configuration array.
     *
     * @param  array<string, mixed>  $config
     * @return string
     */
    public static function buildOracle(array $config): string
    {
        $database = is_scalar($config['database'] ?? null) ? (string) $config['database'] : '';
        $charset = is_scalar($config['charset'] ?? null) ? (string) $config['charset'] : '';
        $host = is_scalar($config['host'] ?? null) ? (string) $config['host'] : '';
        $port = is_numeric($config['port'] ?? null) ? (int) $config['port'] : 0;

        $dsn = 'oci:dbname=';
        if ($host !== '') {
            $dsn .= '//' . $host;
            if ($port > 0) {
                $dsn .= ':' . $port;
            }
            $dsn .= '/';
        }
        $dsn .= $database;
        if ($charset !== '') {
            $dsn .= ';charset=' . $charset;
        }

        return $dsn;
    }

    /**
     * Builds an IBM DB2 DSN string.
     *
     * @param  string  $dsn
     * @return string
     */
    public static function buildIBM(string $dsn): string
    {
        return 'ibm:' . $dsn;
    }

    /**
     * Builds an ODBC DSN string.
     *
     * @param  string  $dsn
     * @return string
     */
    public static function buildODBC(string $dsn): string
    {
        return 'odbc:' . $dsn;
    }

    /**
     * Builds a Firebird DSN string.
     *
     * @param  string  $database
     * @return string
     */
    public static function buildFirebird(string $database): string
    {
        return 'firebird:dbname=' . $database;
    }

    /**
     * Builds an Informix DSN string from a configuration array.
     *
     * @param  array<string, mixed>  $config
     * @return string
     */
    public static function buildInformix(array $config): string
    {
        $dsnParts = [];
        $keys = ['host', 'database', 'server', 'protocol', 'service'];
        foreach ($keys as $key) {
            $value = $config[$key] ?? null;
            if (is_scalar($value) && (string) $value !== '') {
                $dsnParts[] = $key . '=' . (string) $value;
            }
        }

        return 'informix:' . implode(';', $dsnParts);
    }
}