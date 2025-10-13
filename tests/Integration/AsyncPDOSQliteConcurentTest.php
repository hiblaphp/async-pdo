<?php

use Hibla\AsyncPDO\AsyncPDO;
use Hibla\Promise\Promise;

use function Hibla\async;
use function Hibla\await;
use function Hibla\delay;

describe('AsyncPDO Cooperative Query Execution - SQLite', function () {
    beforeEach(function () {
        $config = [
            'driver' => 'sqlite',
            'database' => ':memory:',
        ];

        AsyncPDO::init($config, 20);

        await(async(function () {
            await(AsyncPDO::execute("CREATE TABLE IF NOT EXISTS pool_test (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                data TEXT
            )"));
            await(AsyncPDO::execute("DELETE FROM pool_test"));

            for ($i = 1; $i <= 1000; $i++) {
                await(AsyncPDO::execute(
                    "INSERT INTO pool_test (data) VALUES (?)",
                    ["Row {$i}"]
                ));
            }
        }));
    });

    afterEach(function () {
        await(AsyncPDO::execute("DROP TABLE IF EXISTS pool_test"));
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

                $result = await(AsyncPDO::query("
                    SELECT 
                        t1.id,
                        t1.data,
                        COUNT(t2.id) as count
                    FROM pool_test t1
                    LEFT JOIN pool_test t2 ON t2.id <= t1.id
                    WHERE t1.id BETWEEN " . ($i * 100) . " AND " . ($i * 100 + 50) . "
                    GROUP BY t1.id, t1.data
                    ORDER BY t1.id
                "));

                return count($result);
            });
        }

        $results = await(Promise::all($promises));
        $totalTime = (microtime(true) - $start) * 1000;

        // All queries should start at approximately the same time (within 10ms)
        $maxStartTime = max($startTimes);
        expect($maxStartTime)->toBeLessThan(10);

        // Should return correct row counts
        expect($results)->toEqual([51, 51, 51, 51, 51]);

        // Total time should be less than sequential execution
        // (if blocking, would be ~300ms+, cooperative should be ~60-100ms)
        expect($totalTime)->toBeLessThan(200);
    });

    it('interleaves DB queries with async delays', function () {
        $start = microtime(true);
        $timeline = [];
        $promises = [];

        // 3 DB queries
        for ($i = 1; $i <= 3; $i++) {
            $promises[] = async(function () use ($i, $start, &$timeline) {
                $startTime = microtime(true);
                $timeline["DB-{$i}-start"] = ($startTime - $start) * 1000;

                $result = await(AsyncPDO::query("
                    SELECT * FROM pool_test 
                    WHERE id BETWEEN " . ($i * 100) . " AND " . (($i * 100) + 50) . "
                "));

                $endTime = microtime(true);
                $timeline["DB-{$i}-end"] = ($endTime - $start) * 1000;

                return count($result);
            });
        }

        // 3 Pure async delays
        for ($i = 1; $i <= 3; $i++) {
            $promises[] = async(function () use ($i, $start, &$timeline) {
                $startTime = microtime(true);
                $timeline["DELAY-{$i}-start"] = ($startTime - $start) * 1000;

                await(delay(0.05)); // 50ms delay

                $endTime = microtime(true);
                $timeline["DELAY-{$i}-end"] = ($endTime - $start) * 1000;

                return "delayed";
            });
        }

        await(Promise::all($promises));
        $totalTime = (microtime(true) - $start) * 1000;

        // All operations should start at approximately the same time
        expect($timeline["DB-1-start"])->toBeLessThan(5);
        expect($timeline["DELAY-1-start"])->toBeLessThan(5);

        // DB queries should finish before delays
        expect($timeline["DB-1-end"])->toBeLessThan($timeline["DELAY-1-end"]);

        // Total time should be ~50ms (delay time), not sum of all operations
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

                $result = await(AsyncPDO::fetchOne("
                    SELECT COUNT(*) as total
                    FROM pool_test t1, pool_test t2
                    WHERE t1.id < " . (100 + ($i * 50)) . "
                    AND t2.id < " . (100 + ($i * 50)) . "
                    AND t1.id < t2.id
                "));

                $events['query_end'] = microtime(true) - $start;

                $timeline[$i] = $events;

                return $result['total'];
            });
        }

        await(Promise::all($promises));

        // Check if query executions overlap
        $overlapping = false;
        for ($i = 1; $i <= 2; $i++) {
            $query1_start = $timeline[$i]['query_start'];
            $query1_end = $timeline[$i]['query_end'];
            $query2_start = $timeline[$i + 1]['query_start'];

            if ($query2_start < $query1_end) {
                $overlapping = true;
            }
        }

        // Queries should overlap in cooperative execution
        expect($overlapping)->toBeTrue();

        // All queries should start at approximately the same time
        $maxStartDelay = max(
            $timeline[1]['query_start'],
            $timeline[2]['query_start'],
            $timeline[3]['query_start']
        ) * 1000; // Convert to ms

        expect($maxStartDelay)->toBeLessThan(10);
    });
});
