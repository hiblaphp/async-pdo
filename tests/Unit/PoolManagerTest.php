<?php

use Hibla\AsyncPDO\Manager\PoolManager;
use Hibla\Promise\Interfaces\PromiseInterface;

function getTestConfig(): array
{
    return [
        'driver' => 'sqlite',
        'database' => ':memory:',
    ];
}


describe('Constructor', function () {
    it('creates a pool with valid configuration', function () {
        $pool = new PoolManager(getTestConfig(), 5);
        $stats = $pool->getStats();

        expect($stats['max_size'])->toBe(5)
            ->and($stats['active_connections'])->toBe(0)
            ->and($stats['pooled_connections'])->toBe(0)
            ->and($stats['waiting_requests'])->toBe(0)
            ->and($stats['config_validated'])->toBeTrue();
    });

    it('uses default max size of 10', function () {
        $pool = new PoolManager(getTestConfig());
        $stats = $pool->getStats();

        expect($stats['max_size'])->toBe(10);
    });

    it('throws exception for empty configuration', function () {
        expect(fn() => new PoolManager([]))
            ->toThrow(InvalidArgumentException::class, 'Database configuration cannot be empty');
    });

    it('throws exception for missing driver', function () {
        expect(fn() => new PoolManager(['database' => 'test.db']))
            ->toThrow(InvalidArgumentException::class, "Database configuration field 'driver' must be a non-empty string");
    });

    it('throws exception for empty driver', function () {
        expect(fn() => new PoolManager(['driver' => '']))
            ->toThrow(InvalidArgumentException::class, "Database configuration field 'driver' must be a non-empty string");
    });

    it('throws exception for MySQL without required fields', function () {
        expect(fn() => new PoolManager(['driver' => 'mysql']))
            ->toThrow(InvalidArgumentException::class, "Database configuration field 'host' cannot be empty for driver 'mysql'");
    });

    it('throws exception for PostgreSQL without required fields', function () {
        expect(fn() => new PoolManager(['driver' => 'pgsql', 'host' => 'localhost']))
            ->toThrow(InvalidArgumentException::class, "Database configuration field 'database' cannot be empty for driver 'pgsql'");
    });

    it('throws exception for SQLite without database', function () {
        expect(fn() => new PoolManager(['driver' => 'sqlite']))
            ->toThrow(InvalidArgumentException::class, "Database configuration field 'database' cannot be empty for driver 'sqlite'");
    });

    it('throws exception for invalid port type', function () {
        expect(fn() => new PoolManager([
            'driver' => 'mysql',
            'host' => 'localhost',
            'database' => 'test',
            'port' => 'invalid',
        ]))->toThrow(InvalidArgumentException::class, 'Database port must be a positive integer');
    });

    it('throws exception for negative port', function () {
        expect(fn() => new PoolManager([
            'driver' => 'mysql',
            'host' => 'localhost',
            'database' => 'test',
            'port' => -1,
        ]))->toThrow(InvalidArgumentException::class, 'Database port must be a positive integer');
    });

    it('throws exception for invalid options type', function () {
        expect(fn() => new PoolManager([
            'driver' => 'sqlite',
            'database' => ':memory:',
            'options' => 'not an array',
        ]))->toThrow(InvalidArgumentException::class, 'Database options must be an array');
    });
});

describe('Connection Acquisition', function () {
    it('gets a connection from empty pool', function () {
        $pool = new PoolManager(getTestConfig(), 5);
        $promise = $pool->get();

        expect($promise)->toBeInstanceOf(PromiseInterface::class);

        $connection = $promise->await();
        expect($connection)->toBeInstanceOf(PDO::class);

        $stats = $pool->getStats();
        expect($stats['active_connections'])->toBe(1);
    });

    it('creates new connection when pool is empty', function () {
        $pool = new PoolManager(getTestConfig(), 5);

        $connection = $pool->get()->await();

        expect($connection)->toBeInstanceOf(PDO::class);

        $stats = $pool->getStats();
        expect($stats['active_connections'])->toBe(1)
            ->and($stats['pooled_connections'])->toBe(0);
    });

    it('reuses connection from pool', function () {
        $pool = new PoolManager(getTestConfig(), 5);

        $connection1 = $pool->get()->await();
        $pool->release($connection1);

        $stats = $pool->getStats();
        expect($stats['pooled_connections'])->toBe(1);

        $connection2 = $pool->get()->await();

        expect($connection2)->toBe($connection1); // Same instance

        $stats = $pool->getStats();
        expect($stats['pooled_connections'])->toBe(0)
            ->and($stats['active_connections'])->toBe(1);
    });

    it('creates multiple connections up to max size', function () {
        $pool = new PoolManager(getTestConfig(), 3);
        $connections = [];

        for ($i = 0; $i < 3; $i++) {
            $connections[] = $pool->get()->await();
        }

        expect($connections)->toHaveCount(3);

        foreach ($connections as $conn) {
            expect($conn)->toBeInstanceOf(PDO::class);
        }

        $stats = $pool->getStats();
        expect($stats['active_connections'])->toBe(3);
    });

    it('queues requests when pool is full', function () {
        $pool = new PoolManager(getTestConfig(), 2);
        $connections = [];


        for ($i = 0; $i < 2; $i++) {
            $connections[] = $pool->get()->await();
        }

        $waitingPromise = $pool->get();

        $stats = $pool->getStats();
        expect($stats['waiting_requests'])->toBe(1);
        expect($waitingPromise->isPending())->toBeTrue();

        $pool->release($connections[0]);

        $waitingConnection = $waitingPromise->await();
        expect($waitingConnection)->toBeInstanceOf(PDO::class);

        $stats = $pool->getStats();
        expect($stats['waiting_requests'])->toBe(0);
    });

    it('handles multiple queued requests', function () {
        $pool = new PoolManager(getTestConfig(), 1);

        $connection1 = $pool->get()->await();

        $promises = [];
        for ($i = 0; $i < 3; $i++) {
            $promises[] = $pool->get();
        }

        $stats = $pool->getStats();
        expect($stats['waiting_requests'])->toBe(3);

        $pool->release($connection1);

        $connection2 = $promises[0]->await();
        expect($connection2)->toBeInstanceOf(PDO::class);

        $stats = $pool->getStats();
        expect($stats['waiting_requests'])->toBe(2);
    });

    it('tracks last connection', function () {
        $pool = new PoolManager(getTestConfig(), 5);

        expect($pool->getLastConnection())->toBeNull();

        $connection = $pool->get()->await();

        expect($pool->getLastConnection())->toBe($connection);
    });
});

describe('Connection Release', function () {
    it('returns connection to pool', function () {
        $pool = new PoolManager(getTestConfig(), 5);

        $connection = $pool->get()->await();
        $pool->release($connection);

        $stats = $pool->getStats();
        expect($stats['pooled_connections'])->toBe(1)
            ->and($stats['active_connections'])->toBe(1);
    });

    it('passes connection to waiting request', function () {
        $pool = new PoolManager(getTestConfig(), 1);

        $connection1 = $pool->get()->await();

        $promise = $pool->get();
        expect($promise->isPending())->toBeTrue();

        $stats = $pool->getStats();
        expect($stats['waiting_requests'])->toBe(1);

        $pool->release($connection1);

        $connection2 = $promise->await();
        expect($connection2)->toBe($connection1);

        $stats = $pool->getStats();
        expect($stats['waiting_requests'])->toBe(0)
            ->and($stats['pooled_connections'])->toBe(0);
    });

    it('handles dead connection by removing from pool', function () {
        $pool = new PoolManager(getTestConfig(), 2);

        $connection = $pool->get()->await();
        $initialActive = $pool->getStats()['active_connections'];

        $reflectionClass = new ReflectionClass(PDO::class);
        $deadConnection = $reflectionClass->newInstanceWithoutConstructor();

        $pool->release($deadConnection);

        $stats = $pool->getStats();
        expect($stats['active_connections'])->toBeLessThan($initialActive);
    });

    it('creates new connection for waiter when released connection is dead', function () {
        $pool = new PoolManager(getTestConfig(), 2);

        $connection1 = $pool->get()->await();
        $connection2 = $pool->get()->await();

        $promise = $pool->get();
        expect($promise->isPending())->toBeTrue();

        $stats = $pool->getStats();
        expect($stats['waiting_requests'])->toBe(1);

        $reflectionClass = new ReflectionClass(PDO::class);
        $deadConnection = $reflectionClass->newInstanceWithoutConstructor();
        $pool->release($deadConnection);

        $connection3 = $promise->await();
        expect($connection3)->toBeInstanceOf(PDO::class);
    });

    it('rolls back active transaction before pooling', function () {
        $pool = new PoolManager(getTestConfig(), 5);

        $connection = $pool->get()->await();

        $connection->beginTransaction();
        expect($connection->inTransaction())->toBeTrue();

        $pool->release($connection);

        $connection2 = $pool->get()->await();

        expect($connection2->inTransaction())->toBeFalse();
    });
});

describe('Connection Validation', function () {
    it('validates connection is alive with SELECT 1', function () {
        $pool = new PoolManager(getTestConfig(), 5);

        $connection = $pool->get()->await();

        $result = $connection->query('SELECT 1');
        expect($result)->not->toBeFalse();

        $pool->release($connection);

        $stats = $pool->getStats();
        expect($stats['pooled_connections'])->toBe(1);
    });
});

describe('Pool Statistics', function () {
    it('tracks active connections correctly', function () {
        $pool = new PoolManager(getTestConfig(), 5);

        expect($pool->getStats()['active_connections'])->toBe(0);

        for ($i = 0; $i < 3; $i++) {
            $pool->get()->await();
        }

        expect($pool->getStats()['active_connections'])->toBe(3);
    });

    it('tracks pooled connections correctly', function () {
        $pool = new PoolManager(getTestConfig(), 5);
        $connections = [];

        for ($i = 0; $i < 3; $i++) {
            $connections[] = $pool->get()->await();
        }

        expect($pool->getStats()['pooled_connections'])->toBe(0);

        foreach ($connections as $conn) {
            $pool->release($conn);
        }

        expect($pool->getStats()['pooled_connections'])->toBe(3);
    });

    it('tracks waiting requests correctly', function () {
        $pool = new PoolManager(getTestConfig(), 2);

        for ($i = 0; $i < 2; $i++) {
            $pool->get()->await();
        }

        expect($pool->getStats()['waiting_requests'])->toBe(0);

        for ($i = 0; $i < 3; $i++) {
            $pool->get();
        }

        expect($pool->getStats()['waiting_requests'])->toBe(3);
    });

    it('returns correct stats structure', function () {
        $pool = new PoolManager(getTestConfig(), 5);
        $stats = $pool->getStats();

        expect($stats)->toHaveKeys([
            'active_connections',
            'pooled_connections',
            'waiting_requests',
            'max_size',
            'config_validated',
        ]);
    });
});

describe('Pool Closure', function () {
    it('clears all pooled connections', function () {
        $pool = new PoolManager(getTestConfig(), 5);
        $connections = [];

        for ($i = 0; $i < 3; $i++) {
            $connections[] = $pool->get()->await();
        }

        foreach ($connections as $conn) {
            $pool->release($conn);
        }

        expect($pool->getStats()['pooled_connections'])->toBe(3);

        $pool->close();

        $stats = $pool->getStats();
        expect($stats['pooled_connections'])->toBe(0)
            ->and($stats['active_connections'])->toBe(0);
    });

    it('rejects all waiting requests', function () {
        $pool = new PoolManager(getTestConfig(), 1);

        $pool->get()->await();

        $promises = [];
        for ($i = 0; $i < 3; $i++) {
            $promises[] = $pool->get();
        }

        expect($pool->getStats()['waiting_requests'])->toBe(3);

        $pool->close();

        foreach ($promises as $promise) {
            try {
                $promise->await();
                expect(false)->toBeTrue('Should have thrown an exception');
            } catch (RuntimeException $e) {
                expect($e->getMessage())->toBe('Pool is being closed');
            }
        }
    });

    it('clears last connection', function () {
        $pool = new PoolManager(getTestConfig(), 5);

        $pool->get()->await();
        expect($pool->getLastConnection())->toBeInstanceOf(PDO::class);

        $pool->close();
        expect($pool->getLastConnection())->toBeNull();
    });

    it('resets all counters', function () {
        $pool = new PoolManager(getTestConfig(), 5);

        for ($i = 0; $i < 3; $i++) {
            $pool->get()->await();
        }

        $pool->close();

        $stats = $pool->getStats();
        expect($stats['active_connections'])->toBe(0)
            ->and($stats['pooled_connections'])->toBe(0)
            ->and($stats['waiting_requests'])->toBe(0);
    });
});

describe('Connection Creation', function () {
    it('sets error mode to exception', function () {
        $pool = new PoolManager(getTestConfig(), 5);

        $connection = $pool->get()->await();

        expect($connection->getAttribute(PDO::ATTR_ERRMODE))
            ->toBe(PDO::ERRMODE_EXCEPTION);
    });

    it('applies custom PDO options', function () {
        $config = [
            'driver' => 'sqlite',
            'database' => ':memory:',
            'options' => [
                PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
            ],
        ];

        $pool = new PoolManager($config, 5);

        $connection = $pool->get()->await();

        expect($connection->getAttribute(PDO::ATTR_DEFAULT_FETCH_MODE))
            ->toBe(PDO::FETCH_ASSOC);
    });

    it('handles connection failure gracefully', function () {
        $config = [
            'driver' => 'sqlite',
            'database' => "/\0invalid/path",
        ];

        $pool = new PoolManager($config, 5);

        $exceptionThrown = false;
        try {
            $pool->get()->await();
        } catch (RuntimeException $e) {
            $exceptionThrown = true;
            expect($e->getMessage())->toContain('PDO Connection failed');
        }

        expect($exceptionThrown)->toBeTrue('Should have thrown a RuntimeException');
    });
});

describe('Edge Cases', function () {
    it('handles max size of 1', function () {
        $pool = new PoolManager(getTestConfig(), 1);

        $connection1 = $pool->get()->await();

        $promise = $pool->get();
        expect($pool->getStats()['waiting_requests'])->toBe(1);

        $pool->release($connection1);

        $connection2 = $promise->await();
        expect($connection2)->toBeInstanceOf(PDO::class);
        expect($pool->getStats()['waiting_requests'])->toBe(0);
    });

    it('handles releasing same connection multiple times', function () {
        $pool = new PoolManager(getTestConfig(), 5);

        $connection = $pool->get()->await();

        $pool->release($connection);
        expect($pool->getStats()['pooled_connections'])->toBe(1);

        $pool->release($connection);
        expect($pool->getStats()['pooled_connections'])->toBe(2);
    });

    it('works with actual database operations', function () {
        $pool = new PoolManager(getTestConfig(), 5);

        $connection = $pool->get()->await();

        $connection->exec('CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)');
        $connection->exec("INSERT INTO test (name) VALUES ('Alice')");

        $pool->release($connection);

        $connection2 = $pool->get()->await();

        $result = $connection2->query('SELECT * FROM test')->fetch(PDO::FETCH_ASSOC);
        expect($result['name'])->toBe('Alice');
    });

    it('maintains connection state across pool cycles', function () {
        $pool = new PoolManager(getTestConfig(), 3);

        $conn1 = $pool->get()->await();
        $conn1->exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
        $conn1->exec("INSERT INTO users (name) VALUES ('John')");

        $pool->release($conn1);

        $conn2 = $pool->get()->await();

        expect($conn2)->toBe($conn1);
        $result = $conn2->query('SELECT COUNT(*) as count FROM users')->fetch(PDO::FETCH_ASSOC);
        expect($result['count'])->toBe(1);
    });

    it('handles concurrent requests properly', function () {
        $pool = new PoolManager(getTestConfig(), 2);

        $conn1 = $pool->get()->await();
        $conn2 = $pool->get()->await();

        $promise1 = $pool->get();
        $promise2 = $pool->get();

        expect($pool->getStats()['waiting_requests'])->toBe(2);

        $pool->release($conn1);
        $pool->release($conn2);

        $conn3 = $promise1->await();
        $conn4 = $promise2->await();

        expect($conn3)->toBeInstanceOf(PDO::class);
        expect($conn4)->toBeInstanceOf(PDO::class);
        expect($pool->getStats()['waiting_requests'])->toBe(0);
    });
});
