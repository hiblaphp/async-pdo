<?php

declare(strict_types=1);

use Hibla\AsyncPDO\AsyncPDOConnection;
use Tests\Helpers\StressTestHelper;

describe('AsyncPDO Stress Test - PostgreSQL', function () {
    /** @var AsyncPDOConnection */
    $db = null;

    beforeEach(function () use (&$db) {
        if (empty(getenv('PGSQL_HOST'))) {
            test()->markTestSkipped('PostgreSQL not configured');
        }

        $config = [
            'driver' => 'pgsql',
            'host' => getenv('PGSQL_HOST') ?: 'localhost',
            'port' => (int) (getenv('PGSQL_PORT') ?: 5432),
            'database' => getenv('PGSQL_DATABASE') ?: 'test',
            'username' => getenv('PGSQL_USERNAME') ?: 'postgres',
            'password' => getenv('PGSQL_PASSWORD') ?: '',
        ];

        $db = new AsyncPDOConnection($config, 50);

        StressTestHelper::setupPostgreSQLSchema($db)->await();
    });

    afterEach(function () use (&$db) {
        if ($db !== null) {
            $db->execute('DROP TABLE IF EXISTS order_items CASCADE')->await();
            $db->execute('DROP TABLE IF EXISTS orders CASCADE')->await();
            $db->execute('DROP TABLE IF EXISTS users CASCADE')->await();
            $db->execute('DROP TYPE IF EXISTS order_status CASCADE')->await();
            $db->reset();
            $db = null;
        }
    });

    it('handles light load with 10 concurrent users', function () use (&$db) {
        $result = StressTestHelper::runStressTest($db, 10, 2)->await();

        expect($result['successful_operations'])->toBeGreaterThan(0);
        expect($result['operations_per_second'])->toBeGreaterThan(0);
        expect($result['concurrency_ratio'])->toBeGreaterThan(0.5);
    });

    it('handles medium load with 25 concurrent users', function () use (&$db) {
        $result = StressTestHelper::runStressTest($db, 25, 8)->await();

        expect($result['successful_operations'])->toBeGreaterThan(0);
        expect($result['operations_per_second'])->toBeGreaterThan(0);
        expect($result['concurrency_ratio'])->toBeGreaterThan(0.5);
    });

    it('handles heavy load with 50 concurrent users', function () use (&$db) {
        $result = StressTestHelper::runStressTest($db, 50, 10)->await();

        expect($result['successful_operations'])->toBeGreaterThan(0);
        expect($result['operations_per_second'])->toBeGreaterThan(0);
        expect($result['concurrency_ratio'])->toBeGreaterThan(0.5);
    });

    it('handles extreme load with 100 concurrent users', function () use (&$db) {
        $result = StressTestHelper::runStressTest($db, 100, 10)->await();

        expect($result['successful_operations'])->toBeGreaterThan(0);
        expect($result['operations_per_second'])->toBeGreaterThan(0);
    });

    it('executes user registration workflow', function () use (&$db) {
        $userId = StressTestHelper::simulateUserRegistration($db)->await();

        expect($userId)->toBeGreaterThan(0);

        $user = $db->fetchOne('SELECT * FROM users WHERE id = ?', [$userId])->await();
        expect($user)->not->toBeNull();
        expect($user['email'])->toContain('@example.com');
    });

    it('executes order workflow with transactions', function () use (&$db) {
        $userId = StressTestHelper::simulateUserRegistration($db)->await();
        $orderResult = StressTestHelper::simulateOrderWorkflow($db, $userId)->await();

        expect($orderResult['order_id'])->toBeGreaterThan(0);
        expect($orderResult['total'])->toBeGreaterThan(0);
        expect($orderResult['items_count'])->toBeGreaterThan(0);

        $order = $db->fetchOne('SELECT * FROM orders WHERE id = ?', [$orderResult['order_id']])->await();
        expect($order)->not->toBeNull();
        expect(abs((float) $order['total'] - (float) $orderResult['total']))->toBeLessThan(0.01);
    });

    it('executes analytics queries concurrently', function () use (&$db) {
        $userId = StressTestHelper::simulateUserRegistration($db)->await();
        StressTestHelper::simulateOrderWorkflow($db, $userId)->await();

        $results = StressTestHelper::simulateAnalyticsQueries($db)->await();

        expect($results)->toHaveCount(3);
        expect($results[0])->toBeArray();
        expect($results[1])->toBeArray();
        expect($results[2])->toBeArray();
    });

    it('executes heavy read operations', function () use (&$db) {
        $userId = StressTestHelper::simulateUserRegistration($db)->await();
        StressTestHelper::simulateOrderWorkflow($db, $userId)->await();

        $results = StressTestHelper::simulateHeavyReadOperations($db)->await();

        expect($results)->toHaveCount(3);
        foreach ($results as $result) {
            expect($result)->toBeArray();
        }
    });

    it('executes inventory updates', function () use (&$db) {
        $userId = StressTestHelper::simulateUserRegistration($db)->await();
        StressTestHelper::simulateOrderWorkflow($db, $userId)->await();

        $updatedCount = StressTestHelper::simulateInventoryUpdates($db)->await();

        expect($updatedCount)->toBeGreaterThanOrEqual(0);
    });

    it('demonstrates true async behavior with concurrency ratio', function () use (&$db) {
        $result = StressTestHelper::runStressTest($db, 20, 5)->await();

        expect($result['concurrency_ratio'])->toBeGreaterThan(0.8);
        expect($result['total_time_ms'])->toBeLessThan($result['avg_session_time_ms'] * 1.5);
    });
});