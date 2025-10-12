<?php

use Hibla\AsyncPDO\AsyncPDO;
use Hibla\AsyncPDO\DatabaseConfigFactory;
use Hibla\Promise\Promise;

use function Hibla\sleep;

require 'vendor/autoload.php';

AsyncPDO::init(DatabaseConfigFactory::mysql([
    "host" => "localhost",
    "database" => "yo",
    "username" => "root",
    "password" => "Reymart1234",
    "port" => 3309,
    "options" => [PDO::ATTR_PERSISTENT => true],
]), 20);


echo "=== Extreme Test: 1000 Queries ===\n\n";;

Promise::all([AsyncPDO::query("SELECT 1")])->await();

$start = microtime(true);
$queries = [];
for ($i = 0; $i < 100; $i++) {
    $queries[] = async(function () {
        sleep(0.01);
        AsyncPDO::query("SELECT 1");
    });
}

Promise::all($queries)->await();
$asyncTime = microtime(true) - $start;
echo "AsyncPDO: " . $asyncTime . " seconds\n";

$pdo = new PDO("mysql:host=localhost;port=3309;dbname=yo", "root", "Reymart1234", [PDO::ATTR_PERSISTENT => true]);
$start = microtime(true);
for ($i = 0; $i < 100; $i++) {
    $pdo->query("SELECT SLEEP(0.01)")->fetchAll();
}
$pdoTime = microtime(true) - $start;
echo "Pure PDO: " . $pdoTime . " seconds\n\n";

echo "AsyncPDO should be ~" . round($pdoTime / $asyncTime, 2) . "x faster\n";
