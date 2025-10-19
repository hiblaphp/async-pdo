<?php

use function Hibla\async;

use Hibla\AsyncPDO\AsyncPDO;

use function Hibla\await;
use function Hibla\delay;

use Hibla\Task\Task;

describe('AsyncPDO Cooperative Query Execution - SQL Server', function () {
    beforeEach(function () {
        skipIfPhp84OrHigher();

        if (empty($_ENV['MSSQL_HOST'])) {
            test()->markTestSkipped('SQL Server not configured');
        }

        $config = [
            'driver' => 'sqlsrv',
            'host' => $_ENV['MSSQL_HOST'] ?? 'localhost',
            'port' => (int) ($_ENV['MSSQL_PORT'] ?? 1433),
            'database' => $_ENV['MSSQL_DATABASE'] ?? 'master',
            'username' => $_ENV['MSSQL_USERNAME'] ?? 'sa',
            'password' => $_ENV['MSSQL_PASSWORD'] ?? '',
        ];

        AsyncPDO::init($config, 20);

        Task::run(function () {
            await(AsyncPDO::execute('IF OBJECT_ID(\'pool_test\', \'U\') IS NOT NULL DROP TABLE pool_test'));

            await(AsyncPDO::execute('CREATE TABLE pool_test (
            id INT PRIMARY KEY IDENTITY(1,1),
            data VARCHAR(255)
        )'));

            for ($i = 1; $i <= 1000; $i++) {
                await(AsyncPDO::execute(
                    'INSERT INTO pool_test (data) VALUES (?)',
                    ["Row {$i}"]
                ));
            }
        });
    });

    afterEach(function () {
        await(AsyncPDO::execute('DROP TABLE IF EXISTS pool_test'));
        AsyncPDO::reset();
    });

    it('executes queries cooperatively with interleaving starts', function () {
        $start = microtime(true);
        $startTimes = [];
        $promises = [];

        for ($i = 1; $i <= 5; $i++) {
            $promises[] = async(function () use ($i, $start, &$startTimes) {
                $startTime = microtime(true);
                $startTimes[$i] = ($startTime - $start) * 1000;

                $result = await(AsyncPDO::query('
                    SELECT 
                        t1.id,
                        t1.data,
                        COUNT(t2.id) as count
                    FROM pool_test t1
                    LEFT JOIN pool_test t2 ON t2.id <= t1.id
                    WHERE t1.id BETWEEN '.($i * 100).' AND '.(($i * 100) + 50).'
                    GROUP BY t1.id, t1.data
                    ORDER BY t1.id
                '));

                return count($result);
            });
        }

        $results = Task::runAll($promises);
        $totalTime = (microtime(true) - $start) * 1000;

        $maxStartTime = max($startTimes);
        expect($maxStartTime)->toBeLessThan(10);
        expect($results)->toEqual([51, 51, 51, 51, 51]);
        expect($totalTime)->toBeLessThan(300);
    });

    it('interleaves DB queries with async delays', function () {
        $start = microtime(true);
        $timeline = [];
        $promises = [];

        for ($i = 1; $i <= 3; $i++) {
            $promises[] = async(function () use ($i, $start, &$timeline) {
                $startTime = microtime(true);
                $timeline["DB-{$i}-start"] = ($startTime - $start) * 1000;

                $result = await(AsyncPDO::query('
                    SELECT * FROM pool_test 
                    WHERE id BETWEEN '.($i * 100).' AND '.(($i * 100) + 50).'
                '));

                $endTime = microtime(true);
                $timeline["DB-{$i}-end"] = ($endTime - $start) * 1000;

                return count($result);
            });
        }

        for ($i = 1; $i <= 3; $i++) {
            $promises[] = async(function () use ($i, $start, &$timeline) {
                $startTime = microtime(true);
                $timeline["DELAY-{$i}-start"] = ($startTime - $start) * 1000;

                await(delay(0.05));

                $endTime = microtime(true);
                $timeline["DELAY-{$i}-end"] = ($endTime - $start) * 1000;

                return 'delayed';
            });
        }

        Task::runAll($promises);
        $totalTime = (microtime(true) - $start) * 1000;

        expect($timeline['DB-1-start'])->toBeLessThan(5);
        expect($timeline['DELAY-1-start'])->toBeLessThan(5);
        expect($timeline['DB-1-end'])->toBeLessThan($timeline['DELAY-1-end']);
        expect($totalTime)->toBeLessThan(100);
    });

    it('shows query execution overlap in timestamps', function () {
        $start = microtime(true);
        $timeline = [];
        $promises = [];

        for ($i = 1; $i <= 3; $i++) {
            $promises[] = async(function () use ($i, $start, &$timeline) {
                $events = [];

                $events['fiber_start'] = microtime(true) - $start;
                $events['query_start'] = microtime(true) - $start;

                $maxId = 100 + $i * 50;

                $result = await(AsyncPDO::fetchOne('
                    SELECT COUNT(*) as total
                    FROM pool_test t1, pool_test t2
                    WHERE t1.id < '.(100 + $i * 50).'
                    AND t2.id < '.(100 + $i * 50).'
                    AND t1.id < t2.id
                '));

                $events['query_end'] = microtime(true) - $start;

                $timeline[$i] = $events;

                return $result['total'];
            });
        }

        Task::runAll($promises);

        $overlapping = false;
        for ($i = 1; $i <= 2; $i++) {
            $query1_end = $timeline[$i]['query_end'];
            $query2_start = $timeline[$i + 1]['query_start'];

            if ($query2_start < $query1_end) {
                $overlapping = true;
            }
        }

        expect($overlapping)->toBeTrue();

        $maxStartDelay = max(
            $timeline[1]['query_start'],
            $timeline[2]['query_start'],
            $timeline[3]['query_start']
        ) * 1000;

        expect($maxStartDelay)->toBeLessThan(10);
    });
});
