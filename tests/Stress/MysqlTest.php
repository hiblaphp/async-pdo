<?php

use Hibla\AsyncPDO\AsyncPDO;

use function Hibla\await;

use Hibla\Task\Task;
use Tests\Helpers\StressTestHelper;

describe('AsyncPDO Stress Test - MySQL', function () {
    beforeEach(function () {
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

        AsyncPDO::init($config, 50);

        StressTestHelper::setupMySQLSchema()->await();
    });

    afterEach(function () {
        Task::run(function () {
            await(AsyncPDO::execute('DROP TABLE IF EXISTS order_items'));
            await(AsyncPDO::execute('DROP TABLE IF EXISTS orders'));
            await(AsyncPDO::execute('DROP TABLE IF EXISTS users'));
        });
        AsyncPDO::reset();
    });

    it('handles light load with 10 concurrent users', function () {
        $result = StressTestHelper::runStressTest(10, 2)->await();

        expect($result['successful_operations'])->toBeGreaterThan(0);
        expect($result['operations_per_second'])->toBeGreaterThan(0);
        expect($result['concurrency_ratio'])->toBeGreaterThan(0.5);
    });

    it('handles medium load with 25 concurrent users', function () {
        $result = StressTestHelper::runStressTest(25, 8)->await();

        expect($result['successful_operations'])->toBeGreaterThan(0);
        expect($result['operations_per_second'])->toBeGreaterThan(0);
        expect($result['concurrency_ratio'])->toBeGreaterThan(0.5);
    });

    it('handles heavy load with 50 concurrent users', function () {
        $result = StressTestHelper::runStressTest(50, 10)->await();

        expect($result['successful_operations'])->toBeGreaterThan(0);
        expect($result['operations_per_second'])->toBeGreaterThan(0);
        expect($result['concurrency_ratio'])->toBeGreaterThan(0.5);
    });

    it('handles extreme load with 100 concurrent users', function () {
        $result = StressTestHelper::runStressTest(100, 10)->await();

        expect($result['successful_operations'])->toBeGreaterThan(0);
        expect($result['operations_per_second'])->toBeGreaterThan(0);
    });

    it('executes user registration workflow', function () {
        $userId = StressTestHelper::simulateUserRegistration()->await();

        expect($userId)->toBeGreaterThan(0);

        $user = await(AsyncPDO::fetchOne('SELECT * FROM users WHERE id = ?', [$userId]));
        expect($user)->not->toBeNull();
        expect($user['email'])->toContain('@example.com');
    });

    it('executes order workflow with transactions', function () {
        $userId = StressTestHelper::simulateUserRegistration()->await();
        $orderResult = StressTestHelper::simulateOrderWorkflow($userId)->await();

        expect($orderResult['order_id'])->toBeGreaterThan(0);
        expect($orderResult['total'])->toBeGreaterThan(0);
        expect($orderResult['items_count'])->toBeGreaterThan(0);

        $order = await(AsyncPDO::fetchOne('SELECT * FROM orders WHERE id = ?', [$orderResult['order_id']]));
        expect($order)->not->toBeNull();
        expect((float) $order['total'])->toBe($orderResult['total']);
    });

    it('executes analytics queries concurrently', function () {
        $userId = StressTestHelper::simulateUserRegistration()->await();
        StressTestHelper::simulateOrderWorkflow($userId)->await();

        $results = StressTestHelper::simulateAnalyticsQueries()->await();

        expect($results)->toHaveCount(3);
        expect($results[0])->toBeArray();
        expect($results[1])->toBeArray();
        expect($results[2])->toBeArray();
    });

    it('executes heavy read operations', function () {
        $userId = StressTestHelper::simulateUserRegistration()->await();
        StressTestHelper::simulateOrderWorkflow($userId)->await();

        $results = StressTestHelper::simulateHeavyReadOperations()->await();

        expect($results)->toHaveCount(3);
        foreach ($results as $result) {
            expect($result)->toBeArray();
        }
    });

    it('executes inventory updates', function () {
        $userId = StressTestHelper::simulateUserRegistration()->await();
        StressTestHelper::simulateOrderWorkflow($userId)->await();

        $updatedCount = StressTestHelper::simulateInventoryUpdates()->await();

        expect($updatedCount)->toBeGreaterThanOrEqual(0);
    });

    it('demonstrates true async behavior with concurrency ratio', function () {
        $result = StressTestHelper::runStressTest(20, 5)->await();

        expect($result['concurrency_ratio'])->toBeGreaterThan(0.8);
        expect($result['total_time_ms'])->toBeLessThan($result['avg_session_time_ms'] * 1.5);
    });
});
