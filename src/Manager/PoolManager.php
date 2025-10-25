<?php

declare(strict_types=1);

namespace Hibla\AsyncPDO\Manager;

use Hibla\AsyncPDO\Exceptions\ConnectionPoolException;
use Hibla\AsyncPDO\Utilities\DSNBuilder;
use Hibla\Promise\Interfaces\PromiseInterface;
use Hibla\Promise\Promise;
use InvalidArgumentException;
use PDO;
use PDOException;
use RuntimeException;
use SplQueue;
use Throwable;

class PoolManager
{
    /**
     * @var SplQueue<PDO> A queue of available, idle connections.
     */
    private SplQueue $pool;

    /**
     * @var SplQueue<Promise<PDO>> A queue of pending requests (waiters) for a connection.
     */
    private SplQueue $waiters;

    /**
     * @var int The maximum number of concurrent connections allowed.
     */
    private int $maxSize;

    /**
     * @var int The current number of active connections (both in pool and in use).
     */
    private int $activeConnections = 0;

    /**
     * @var array<string, mixed> The database connection configuration.
     */
    private array $dbConfig;

    /**
     * @var bool Flag indicating if the initial configuration was validated.
     */
    private bool $configValidated = false;

    /**
     * @var PDO|null The most recently used or created connection.
     */
    private ?PDO $lastConnection = null;

    /**
     * Creates a new connection pool.
     *
     * @param  array<string, mixed>  $dbConfig  The database configuration array, compatible with PDO.
     * @param  int  $maxSize  The maximum number of concurrent connections allowed.
     *
     * @throws InvalidArgumentException When the database configuration is invalid or maxSize is less than 1.
     */
    public function __construct(array $dbConfig, int $maxSize = 10)
    {
        if ($maxSize < 1) {
            throw new InvalidArgumentException('Pool size must be at least 1');
        }

        $this->validateDbConfig($dbConfig);
        $this->configValidated = true;
        $this->dbConfig = $dbConfig;
        $this->maxSize = $maxSize;
        $this->pool = new SplQueue();
        $this->waiters = new SplQueue();
    }

    /**
     * Asynchronously acquires a PDO connection from the pool.
     *
     * If a connection is available, it returns an instantly resolved promise.
     * If the pool is full, it returns a promise that will resolve when a
     * connection is released by another fiber.
     *
     * @return PromiseInterface<PDO> A promise that resolves with a PDO connection object.
     */
    public function get(): PromiseInterface
    {
        if (! $this->pool->isEmpty()) {
            /** @var PDO $connection */
            $connection = $this->pool->dequeue();
            $this->lastConnection = $connection;

            /** @var PromiseInterface<PDO> $promise */
            $promise = Promise::resolved($connection);

            return $promise;
        }

        if ($this->activeConnections < $this->maxSize) {
            $this->activeConnections++;

            try {
                $connection = $this->createConnection();
                $this->lastConnection = $connection;

                /** @var PromiseInterface<PDO> $promise */
                $promise = Promise::resolved($connection);

                return $promise;
            } catch (Throwable $e) {
                $this->activeConnections--;
                /** @var PromiseInterface<PDO> $promise */
                $promise = Promise::rejected($e);

                return $promise;
            }
        }

        /** @var Promise<PDO> $promise */
        $promise = new Promise();
        $this->waiters->enqueue($promise);

        return $promise;
    }

    /**
     * Releases a PDO connection back to the pool for reuse.
     *
     * If other fibers are waiting for a connection, the connection is passed
     * directly to the next waiting fiber. Otherwise, it's returned to the
     * idle pool.
     *
     * @param  PDO  $connection  The PDO connection to release.
     */
    public function release(PDO $connection): void
    {
        if (! $this->isConnectionAlive($connection)) {
            $this->activeConnections--;
            if (! $this->waiters->isEmpty() && $this->activeConnections < $this->maxSize) {
                $this->activeConnections++;
                /** @var Promise<PDO> $promise */
                $promise = $this->waiters->dequeue();

                try {
                    $newConnection = $this->createConnection();
                    $this->lastConnection = $newConnection;
                    $promise->resolve($newConnection);
                } catch (Throwable $e) {
                    $this->activeConnections--;
                    $promise->reject($e);
                }
            }

            return;
        }

        $this->resetConnectionState($connection);

        if (! $this->waiters->isEmpty()) {
            /** @var Promise<PDO> $promise */
            $promise = $this->waiters->dequeue();
            $this->lastConnection = $connection;
            $promise->resolve($connection);
        } else {
            $this->pool->enqueue($connection);
        }
    }

    /**
     * Gets the most recently active connection handled by the pool.
     *
     * @return PDO|null The last connection object or null if none have been handled.
     */
    public function getLastConnection(): ?PDO
    {
        return $this->lastConnection;
    }

    /**
     * Retrieves statistics about the current state of the connection pool.
     *
     * @return array<string, int|bool> An associative array with pool metrics.
     */
    public function getStats(): array
    {
        return [
            'active_connections' => $this->activeConnections,
            'pooled_connections' => $this->pool->count(),
            'waiting_requests' => $this->waiters->count(),
            'max_size' => $this->maxSize,
            'config_validated' => $this->configValidated,
        ];
    }

    /**
     * Closes all connections and shuts down the pool.
     *
     * This method rejects any pending connection requests and clears the pool.
     * The pool is reset to an empty state and cannot be used until re-initialized.
     */
    public function close(): void
    {
        while (! $this->pool->isEmpty()) {
            $this->pool->dequeue();
        }
        while (! $this->waiters->isEmpty()) {
            /** @var Promise<PDO> $promise */
            $promise = $this->waiters->dequeue();
            $promise->reject(new ConnectionPoolException('Pool is being closed'));
        }
        $this->pool = new SplQueue();
        $this->waiters = new SplQueue();
        $this->activeConnections = 0;
        $this->lastConnection = null;
    }

    /**
     * Validates the provided database configuration array.
     *
     * @param  array<string, mixed>  $dbConfig
     *
     * @throws InvalidArgumentException
     */
    private function validateDbConfig(array $dbConfig): void
    {
        if (count($dbConfig) === 0) {
            throw new InvalidArgumentException('Database configuration cannot be empty');
        }
        if (! isset($dbConfig['driver']) || ! is_string($dbConfig['driver']) || $dbConfig['driver'] === '') {
            throw new InvalidArgumentException("Database configuration field 'driver' must be a non-empty string");
        }
        $this->validateDriverSpecificConfig($dbConfig);
        if (isset($dbConfig['port']) && (! is_int($dbConfig['port']) || $dbConfig['port'] <= 0)) {
            throw new InvalidArgumentException('Database port must be a positive integer');
        }
        if (isset($dbConfig['options']) && ! is_array($dbConfig['options'])) {
            throw new InvalidArgumentException('Database options must be an array');
        }
    }

    /**
     * Validates driver-specific configuration requirements.
     *
     * @param  array<string, mixed>  $dbConfig
     *
     * @throws InvalidArgumentException
     */
    private function validateDriverSpecificConfig(array $dbConfig): void
    {
        /** @var string $driver */
        $driver = $dbConfig['driver'];
        switch (strtolower($driver)) {
            case 'mysql':
            case 'pgsql':
            case 'postgresql':
                $this->validateRequiredFields($dbConfig, ['host', 'database']);

                break;
            case 'sqlite':
            case 'firebird':
            case 'informix':
            case 'oci':
            case 'oracle':
                $this->validateRequiredFields($dbConfig, ['database']);

                break;
            case 'sqlsrv':
            case 'mssql':
                $this->validateRequiredFields($dbConfig, ['host']);

                break;
            case 'ibm':
            case 'db2':
            case 'odbc':
                if (! isset($dbConfig['database']) && ! isset($dbConfig['dsn'])) {
                    throw new InvalidArgumentException("Driver '{$driver}' requires either 'database' or 'dsn' field");
                }

                break;
            default:
        }
    }

    /**
     * Validates that required fields are present and not empty in the configuration.
     *
     * @param  array<string, mixed>  $dbConfig  The configuration to check.
     * @param  list<string>  $requiredFields  A list of keys that must exist and be non-empty.
     *
     * @throws InvalidArgumentException
     */
    private function validateRequiredFields(array $dbConfig, array $requiredFields): void
    {
        /** @var string $driver */
        $driver = $dbConfig['driver'];
        foreach ($requiredFields as $field) {
            if (! array_key_exists($field, $dbConfig) || $dbConfig[$field] === '' || $dbConfig[$field] === null) {
                throw new InvalidArgumentException("Database configuration field '{$field}' cannot be empty for driver '{$driver}'");
            }
        }
    }

    /**
     * Establishes a new PDO connection.
     *
     * @return PDO The newly created connection object.
     *
     * @throws RuntimeException If the connection fails.
     */
    private function createConnection(): PDO
    {
        $dsn = DSNBuilder::build($this->dbConfig);
        $username = isset($this->dbConfig['username']) && is_string($this->dbConfig['username']) ? $this->dbConfig['username'] : null;
        $password = isset($this->dbConfig['password']) && is_string($this->dbConfig['password']) ? $this->dbConfig['password'] : null;
        $options = isset($this->dbConfig['options']) && is_array($this->dbConfig['options']) ? $this->dbConfig['options'] : [];

        try {
            $pdo = new PDO($dsn, $username, $password, $options);
            $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

            return $pdo;
        } catch (PDOException $e) {
            throw new ConnectionPoolException('PDO Connection failed: '.$e->getMessage(), (int) $e->getCode(), $e);
        }
    }

    /**
     * Checks if a PDO connection is still active and usable.
     *
     * @param  PDO  $connection  The connection to check.
     * @return bool True if the connection is alive.
     */
    private function isConnectionAlive(PDO $connection): bool
    {
        try {
            return $connection->query('SELECT 1') !== false;
        } catch (Throwable $e) {
            return false;
        }
    }

    /**
     * Resets the state of a connection before returning it to the pool.
     *
     * @param  PDO  $connection  The connection to reset.
     */
    private function resetConnectionState(PDO $connection): void
    {
        try {
            if ($connection->inTransaction()) {
                $connection->rollBack();
            }
        } catch (Throwable $e) {
            // isConnectionAlive will catch this on the next cycle.
        }
    }
}
