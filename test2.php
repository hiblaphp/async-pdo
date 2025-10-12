<?php

require_once 'vendor/autoload.php';

use Hibla\AsyncPDO\AsyncPDO;
use function Hibla\async;
use function Hibla\await;
use function Hibla\delay;

$config = [
    "driver" => "mysql",
    "host" => "localhost",
    "database" => "yo",
    "username" => "root",
    "password" => "Reymart1234",
    "port" => 3309,
    "options" => [\PDO::ATTR_PERSISTENT => true],
];

echo "=== DEFINITIVE TEST: Are PDO Queries Cooperative(using AsyncPDO Class)? ===\n\n";

AsyncPDO::init($config, 20);

// Setup
await(async(function() {
    await(AsyncPDO::execute("CREATE TABLE IF NOT EXISTS pool_test (
        id INT AUTO_INCREMENT PRIMARY KEY,
        data VARCHAR(255)
    )"));
    await(AsyncPDO::execute("TRUNCATE TABLE pool_test"));
    
    for ($i = 1; $i <= 1000; $i++) {
        await(AsyncPDO::execute(
            "INSERT INTO pool_test (data) VALUES (?)",
            ["Row {$i}"]
        ));
    }
    
    echo "✓ Setup complete\n\n";
}));

// =================================================================
// TEST 1: The Smoking Gun Test
// If queries are cooperative, they should interleave
// If blocking, they'll run sequentially
// =================================================================

echo "TEST 1: The Smoking Gun - Do queries interleave?\n";
echo "If cooperative: All should START at ~0ms\n";
echo "If blocking: Starts will be staggered (0ms, 50ms, 100ms...)\n\n";

$start = microtime(true);
$promises = [];

for ($i = 1; $i <= 5; $i++) {
    $promises[] = async(function() use ($i, $start) {
        $startTime = microtime(true);
        echo "[{$i}] 🚀 STARTED at " . round(($startTime - $start) * 1000) . "ms\n";
        
        // Heavy query to make timing visible
        $beforeQuery = microtime(true);
        $result = await(AsyncPDO::query("
            SELECT 
                t1.id,
                t1.data,
                COUNT(t2.id) as count
            FROM pool_test t1
            LEFT JOIN pool_test t2 ON t2.id <= t1.id
            WHERE t1.id BETWEEN " . ($i * 100) . " AND " . (($i * 100) + 50) . "
            GROUP BY t1.id, t1.data
            ORDER BY t1.id
        "));
        $afterQuery = microtime(true);
        
        echo "[{$i}] 📊 Query executed: " . round(($afterQuery - $beforeQuery) * 1000) . "ms\n";
        
        $endTime = microtime(true);
        echo "[{$i}] ✅ FINISHED at " . round(($endTime - $start) * 1000) . "ms (total: " . 
             round(($endTime - $startTime) * 1000) . "ms)\n\n";
        
        return count($result);
    });
}

$results = await(\Hibla\Promise\Promise::all($promises));
$totalTime = microtime(true) - $start;

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n";
echo "Total time: " . round($totalTime * 1000) . "ms\n";
echo "Results: " . json_encode($results) . "\n\n";

// =================================================================
// TEST 2: Mix DB with Pure Async - The Control Group
// This MUST show interleaving if async system works
// =================================================================

echo "TEST 2: Control Group - DB queries mixed with delay()\n";
echo "This MUST show interleaving to prove async system works\n\n";

$start = microtime(true);
$promises = [];

// 3 DB queries
for ($i = 1; $i <= 3; $i++) {
    $promises[] = async(function() use ($i, $start) {
        $startTime = microtime(true);
        echo "[DB-{$i}] 🚀 STARTED at " . round(($startTime - $start) * 1000) . "ms\n";
        
        $result = await(AsyncPDO::query("
            SELECT * FROM pool_test 
            WHERE id BETWEEN " . ($i * 100) . " AND " . (($i * 100) + 50) . "
        "));
        
        $endTime = microtime(true);
        echo "[DB-{$i}] ✅ FINISHED at " . round(($endTime - $start) * 1000) . "ms\n";
        
        return count($result);
    });
}

// 3 Pure async delays (these MUST interleave)
for ($i = 1; $i <= 3; $i++) {
    $promises[] = async(function() use ($i, $start) {
        $startTime = microtime(true);
        echo "[DELAY-{$i}] 🚀 STARTED at " . round(($startTime - $start) * 1000) . "ms\n";
        
        await(delay(0.05)); // 50ms delay
        
        $endTime = microtime(true);
        echo "[DELAY-{$i}] ✅ FINISHED at " . round(($endTime - $start) * 1000) . "ms\n";
        
        return "delayed";
    });
}

$results = await(\Hibla\Promise\Promise::all($promises));
$totalTime = microtime(true) - $start;

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n";
echo "Total time: " . round($totalTime * 1000) . "ms\n\n";

// =================================================================
// TEST 3: The Ultimate Test - Timestamp Tracking
// Track exact moments to see if queries overlap
// =================================================================

echo "TEST 3: Timestamp Analysis - Do query executions overlap?\n\n";

$start = microtime(true);
$timeline = [];
$promises = [];

for ($i = 1; $i <= 3; $i++) {
    $promises[] = async(function() use ($i, $start, &$timeline) {
        $events = [];
        
        $events['fiber_start'] = microtime(true) - $start;
        
        // Mark query execution start
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

$results = await(\Hibla\Promise\Promise::all($promises));

echo "Timeline (all times in ms):\n";
foreach ($timeline as $id => $events) {
    echo "\nQuery {$id}:\n";
    foreach ($events as $event => $time) {
        echo "  {$event}: " . round($time * 1000, 2) . "ms\n";
    }
    echo "  Query execution time: " . round(($events['query_end'] - $events['query_start']) * 1000, 2) . "ms\n";
}

echo "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n";
echo "Analysis:\n";

// Check if query executions overlap
$overlapping = false;
for ($i = 1; $i <= 2; $i++) {
    $query1_start = $timeline[$i]['query_start'];
    $query1_end = $timeline[$i]['query_end'];
    $query2_start = $timeline[$i + 1]['query_start'];
    
    if ($query2_start < $query1_end) {
        $overlapping = true;
        echo "✅ Query {$i} and Query " . ($i + 1) . " execution OVERLAPPED!\n";
        echo "   Query {$i} was still executing when Query " . ($i + 1) . " started\n";
    } else {
        echo "❌ Query {$i} finished BEFORE Query " . ($i + 1) . " started\n";
        echo "   Gap: " . round(($query2_start - $query1_end) * 1000, 2) . "ms\n";
    }
}

echo "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n";
echo "\n🎯 FINAL VERDICT:\n";

if ($overlapping) {
    echo "✅ PDO queries ARE COOPERATIVE!\n";
    echo "   Query executions overlapped in time\n";
    echo "   Multiple queries ran simultaneously\n";
    echo "   Your pool enables true concurrent database access!\n";
} else {
    echo "❌ PDO queries are BLOCKING\n";
    echo "   Each query waited for the previous to complete\n";
    echo "   No overlap in execution times\n";
    echo "   Queries run sequentially despite async/await\n";
}

echo "\n";

// Cleanup
await(AsyncPDO::execute("DROP TABLE IF EXISTS pool_test"));

AsyncPDO::reset();

echo "Test complete!\n";