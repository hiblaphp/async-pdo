<?php

use Hibla\AsyncPDO\AsyncPDO;
use Hibla\AsyncPDO\DatabaseConfigFactory;
use Hibla\Promise\Promise;

require 'vendor/autoload.php';

// Test configuration constants (identical to Amp test)
const TEST_CONFIGS = [100, 500, 1000, 3000];
const ROUNDS_PER_CONFIG = 5;
const CONNECTION_POOL_SIZE = 50;
const DELAY_BETWEEN_ROUNDS = 2.0;

// Memory tracking function (identical to Amp test)
function getMemoryUsage()
{
    return [
        'current' => memory_get_usage(true),
        'peak' => memory_get_peak_usage(true),
        'current_mb' => round(memory_get_usage(true) / 1024 / 1024, 2),
        'peak_mb' => round(memory_get_peak_usage(true) / 1024 / 1024, 2)
    ];
}

// Function to format large numbers (identical to Amp test)
function formatNumber($number)
{
    return number_format($number, 0);
}

// Function to run a stress test round with FiberAsync
function runStressTestRound($queryCount, int $roundNumber)
{
    echo "  Round $roundNumber: Running " . formatNumber($queryCount) . " concurrent queries...\n";

    return async(function () use ($queryCount) {
        $memoryStart = getMemoryUsage();
        $startTime = microtime(true);

        $promises = [];

        // Pre-calculate query strings to avoid string concatenation in loop
        $baseQuery = "SELECT ";
        $queryTemplate = " as query_id, NOW() as timestamp, 'stress_test' as test_type";

        for ($i = 1; $i <= $queryCount; $i++) {
            $query = $baseQuery . $i . $queryTemplate;
            $promises[] = AsyncPDO::query($query);
        }

        $results = await(Promise::all($promises));
        $endTime = microtime(true);
        $memoryEnd = getMemoryUsage();

        $totalTime = $endTime - $startTime;
        $qps = $queryCount / $totalTime;
        $memoryIncrease = $memoryEnd['current_mb'] - $memoryStart['current_mb'];

        echo "    - Time: " . number_format($totalTime * 1000, 1) . "ms\n";
        echo "    - QPS: " . number_format($qps, 2) . "\n";
        echo "    - Memory: {$memoryEnd['current_mb']}MB (Peak: {$memoryEnd['peak_mb']}MB, +{$memoryIncrease}MB)\n";

        return [
            'time' => $totalTime,
            'qps' => $qps,
            'memory_current' => $memoryEnd['current_mb'],
            'memory_peak' => $memoryEnd['peak_mb'],
            'memory_increase' => $memoryIncrease,
            'successful_queries' => count($results)
        ];
    });
}

// Function to calculate and display averages (identical to Amp test)
function displayAverages(array $results, int $queryCount)
{
    $resultCount = count($results);
    $avgTime = array_sum(array_column($results, 'time')) / $resultCount;
    $avgQps = array_sum(array_column($results, 'qps')) / $resultCount;
    $avgMemoryCurrent = array_sum(array_column($results, 'memory_current')) / $resultCount;
    $avgMemoryPeak = array_sum(array_column($results, 'memory_peak')) / $resultCount;
    $avgMemoryIncrease = array_sum(array_column($results, 'memory_increase')) / $resultCount;
    $totalSuccessful = array_sum(array_column($results, 'successful_queries'));

    echo "\n  AVERAGES ($resultCount rounds):\n";
    echo "  - Average Time: " . number_format($avgTime * 1000, 1) . "ms\n";
    echo "  - Average QPS: " . number_format($avgQps, 2) . "\n";
    echo "  - Average Memory: " . number_format($avgMemoryCurrent, 2) . "MB\n";
    echo "  - Average Peak Memory: " . number_format($avgMemoryPeak, 2) . "MB\n";
    echo "  - Average Memory Increase: " . number_format($avgMemoryIncrease, 2) . "MB\n";
    echo "  - Total Successful Queries: " . formatNumber($totalSuccessful) . "/" . formatNumber($queryCount * $resultCount) . "\n";

    return [
        'query_count' => $queryCount,
        'rounds' => $resultCount,
        'avg_time_ms' => $avgTime * 1000,
        'avg_qps' => $avgQps,
        'avg_memory_mb' => $avgMemoryCurrent,
        'avg_peak_memory_mb' => $avgMemoryPeak,
        'avg_memory_increase_mb' => $avgMemoryIncrease,
        'success_rate' => ($totalSuccessful / ($queryCount * $resultCount)) * 100
    ];
}

echo "=== FiberAsync PDO Comprehensive Stress Test ===\n\n";

// Configuration
$dbConfig = DatabaseConfigFactory::mysql([
    'host' => 'localhost',
    'port' => 3309,
    'database' => 'yo',
    'username' => 'root',
    'password' => 'Reymart1234',
]);

// Initialize the framework's DB layer
AsyncPDO::init($dbConfig, CONNECTION_POOL_SIZE);

run(function () {
    // Warm up
    echo "Warming up connection pool...\n";
    await(AsyncPDO::query("SELECT 1"));
    echo "Warmup completed.\n\n";

    $summaryResults = [];

    echo "Starting comprehensive stress test...\n";
    echo "Test configurations: " . implode(', ', array_map('formatNumber', TEST_CONFIGS)) . " queries\n";
    echo "Rounds per configuration: " . ROUNDS_PER_CONFIG . "\n";
    echo "Connection pool size: " . CONNECTION_POOL_SIZE . "\n\n";

    foreach (TEST_CONFIGS as $queryCount) {
        echo "===========================================\n";
        echo "STRESS TEST: " . formatNumber($queryCount) . " Concurrent Queries\n";
        echo "===========================================\n";

        $roundResults = [];

        for ($round = 1; $round <= ROUNDS_PER_CONFIG; $round++) {
            if ($round > 1) {
                echo "  Waiting " . DELAY_BETWEEN_ROUNDS . " seconds before next round...\n";
                await(delay(DELAY_BETWEEN_ROUNDS));
            }

            $roundResult = await(runStressTestRound($queryCount, $round));
            $roundResults[] = $roundResult;

            echo "\n";
        }

        $summary = displayAverages($roundResults, $queryCount);
        $summaryResults[] = $summary;

        echo "\n";
    }

    //===========================================
    // OVERALL SUMMARY
    //===========================================
    echo "===========================================\n";
    echo "OVERALL PERFORMANCE SUMMARY\n";
    echo "===========================================\n";

    $tableHeader = sprintf(
        "| %-8s | %-7s | %-12s | %-10s | %-12s | %-12s | %-8s |\n",
        "Queries",
        "Rounds",
        "Avg Time(ms)",
        "Avg QPS",
        "Avg Mem(MB)",
        "Peak Mem(MB)",
        "Success%"
    );

    echo $tableHeader;
    echo str_repeat("-", strlen($tableHeader)) . "\n";

    foreach ($summaryResults as $summary) {
        printf(
            "| %-8s | %-7d | %-12s | %-10s | %-12s | %-12s | %-8s |\n",
            formatNumber($summary['query_count']),
            $summary['rounds'],
            number_format($summary['avg_time_ms'], 1),
            number_format($summary['avg_qps'], 2),
            number_format($summary['avg_memory_mb'], 1),
            number_format($summary['avg_peak_memory_mb'], 1),
            number_format($summary['success_rate'], 1) . "%"
        );
    }

    echo "\n";

    //===========================================
    // PERFORMANCE ANALYSIS
    //===========================================
    echo "PERFORMANCE ANALYSIS:\n";
    echo "---------------------\n";

    $bestQps = max(array_column($summaryResults, 'avg_qps'));
    $bestQpsConfig = array_values(array_filter($summaryResults, function ($s) use ($bestQps) {
        return abs($s['avg_qps'] - $bestQps) < 0.01;
    }))[0];

    echo "• Best QPS: " . number_format($bestQps, 2) . " (at " . formatNumber($bestQpsConfig['query_count']) . " queries)\n";

    $memoryUsages = array_column($summaryResults, 'avg_memory_mb');
    $minMemory = min($memoryUsages);
    $maxMemory = max($memoryUsages);
    echo "• Memory Usage Range: " . number_format($minMemory, 1) . "MB - " . number_format($maxMemory, 1) . "MB\n";

    echo "• Throughput Efficiency (QPS per MB):\n";
    foreach ($summaryResults as $summary) {
        $efficiency = $summary['avg_qps'] / $summary['avg_memory_mb'];
        echo "  - " . formatNumber($summary['query_count']) . " queries: " . number_format($efficiency, 2) . " QPS/MB\n";
    }

    echo "• Performance Scaling:\n";
    $summaryCount = count($summaryResults);
    for ($i = 1; $i < $summaryCount; $i++) {
        $prev = $summaryResults[$i - 1];
        $curr = $summaryResults[$i];
        $qpsRatio = $curr['avg_qps'] / $prev['avg_qps'];
        $queryRatio = $curr['query_count'] / $prev['query_count'];
        $efficiency = ($qpsRatio / $queryRatio) * 100;

        echo "  - " . formatNumber($prev['query_count']) . " → " . formatNumber($curr['query_count']) .
            " queries: " . number_format($efficiency, 1) . "% scaling efficiency\n";
    }
});

// Reset the static state, crucial for clean testing environments
AsyncPDO::reset();

echo "\n=== Comprehensive Stress Test Complete ===\n";
// *** THIS IS THE CORRECTED LINE ***
echo "Total queries executed: " . formatNumber(array_sum(TEST_CONFIGS) * ROUNDS_PER_CONFIG) . "\n";
