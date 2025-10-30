<?php

declare(strict_types=1);

namespace Tests\Helpers;

use function Hibla\async;

use Hibla\AsyncPDO\AsyncPDO;

use function Hibla\await;

use Hibla\Promise\Interfaces\PromiseInterface;
use Hibla\Promise\Promise;

class StressTestHelper
{
    private static array $firstNames = ['John', 'Jane', 'Alice', 'Bob', 'Charlie', 'Diana', 'Eve', 'Frank', 'Grace', 'Henry'];
    private static array $lastNames = ['Smith', 'Johnson', 'Brown', 'Davis', 'Wilson', 'Miller', 'Garcia', 'Martinez', 'Anderson', 'Taylor'];
    private static array $cities = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia', 'San Antonio', 'San Diego', 'Dallas', 'San Jose'];
    private static array $products = [
        ['name' => 'Laptop Pro 15"', 'price' => 1299.99, 'category' => 'Electronics'],
        ['name' => 'Wireless Headphones', 'price' => 199.99, 'category' => 'Electronics'],
        ['name' => 'Coffee Maker', 'price' => 89.99, 'category' => 'Kitchen'],
        ['name' => 'Running Shoes', 'price' => 129.99, 'category' => 'Sports'],
        ['name' => 'Office Chair', 'price' => 299.99, 'category' => 'Furniture'],
        ['name' => 'Smartphone', 'price' => 799.99, 'category' => 'Electronics'],
        ['name' => 'Yoga Mat', 'price' => 29.99, 'category' => 'Sports'],
        ['name' => 'Blender', 'price' => 79.99, 'category' => 'Kitchen'],
    ];

    public static function generateUser(): array
    {
        return [
            'first_name' => self::$firstNames[array_rand(self::$firstNames)],
            'last_name' => self::$lastNames[array_rand(self::$lastNames)],
            'email' => uniqid('user_') . '@example.com',
            'city' => self::$cities[array_rand(self::$cities)],
            'created_at' => date('Y-m-d H:i:s'),
        ];
    }

    public static function getRandomProduct(): array
    {
        return self::$products[array_rand(self::$products)];
    }

    public static function generateOrderData(int $userId): array
    {
        $itemCount = rand(1, 3);
        $total = 0;
        $items = [];

        for ($i = 0; $i < $itemCount; $i++) {
            $product = self::getRandomProduct();
            $quantity = rand(1, 2);
            $price = $product['price'];
            $subtotal = $price * $quantity;
            $total += $subtotal;

            $items[] = [
                'product_name' => $product['name'],
                'quantity' => $quantity,
                'price' => $price,
                'subtotal' => $subtotal,
            ];
        }

        return [
            'user_id' => $userId,
            'total' => $total,
            'status' => 'pending',
            'items' => $items,
            'created_at' => date('Y-m-d H:i:s'),
        ];
    }

    public static function setupMySQLSchema(): PromiseInterface
    {
        return async(function () {
            $queries = [
                'DROP TABLE IF EXISTS order_items',
                'DROP TABLE IF EXISTS orders',
                'DROP TABLE IF EXISTS users',
                'CREATE TABLE users (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    first_name VARCHAR(50) NOT NULL,
                    last_name VARCHAR(50) NOT NULL,
                    email VARCHAR(100) UNIQUE NOT NULL,
                    city VARCHAR(50) NOT NULL,
                    created_at DATETIME NOT NULL,
                    INDEX idx_email (email),
                    INDEX idx_city (city)
                )',
                "CREATE TABLE orders (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    user_id INT NOT NULL,
                    total DECIMAL(10,2) NOT NULL,
                    status ENUM('pending', 'processing', 'shipped', 'delivered', 'cancelled') DEFAULT 'pending',
                    created_at DATETIME NOT NULL,
                    FOREIGN KEY (user_id) REFERENCES users(id),
                    INDEX idx_user_id (user_id),
                    INDEX idx_status (status),
                    INDEX idx_created_at (created_at)
                )",
                'CREATE TABLE order_items (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    order_id INT NOT NULL,
                    product_name VARCHAR(100) NOT NULL,
                    quantity INT NOT NULL,
                    price DECIMAL(10,2) NOT NULL,
                    subtotal DECIMAL(10,2) NOT NULL,
                    FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE,
                    INDEX idx_order_id (order_id),
                    INDEX idx_product_name (product_name)
                )',
            ];

            foreach ($queries as $query) {
                await(AsyncPDO::execute($query));
            }
        });
    }

    public static function setupPostgreSQLSchema(): PromiseInterface
    {
        return async(function () {
            $queries = [
                'DROP TABLE IF EXISTS order_items CASCADE',
                'DROP TABLE IF EXISTS orders CASCADE',
                'DROP TABLE IF EXISTS users CASCADE',
                'DROP TYPE IF EXISTS order_status CASCADE',
                "CREATE TYPE order_status AS ENUM ('pending', 'processing', 'shipped', 'delivered', 'cancelled')",
                'CREATE TABLE users (
                    id SERIAL PRIMARY KEY,
                    first_name VARCHAR(50) NOT NULL,
                    last_name VARCHAR(50) NOT NULL,
                    email VARCHAR(100) UNIQUE NOT NULL,
                    city VARCHAR(50) NOT NULL,
                    created_at TIMESTAMP NOT NULL
                )',
                'CREATE INDEX idx_email ON users(email)',
                'CREATE INDEX idx_city ON users(city)',
                "CREATE TABLE orders (
                    id SERIAL PRIMARY KEY,
                    user_id INT NOT NULL,
                    total DECIMAL(10,2) NOT NULL,
                    status order_status DEFAULT 'pending',
                    created_at TIMESTAMP NOT NULL,
                    FOREIGN KEY (user_id) REFERENCES users(id)
                )",
                'CREATE INDEX idx_user_id ON orders(user_id)',
                'CREATE INDEX idx_status ON orders(status)',
                'CREATE INDEX idx_created_at ON orders(created_at)',
                'CREATE TABLE order_items (
                    id SERIAL PRIMARY KEY,
                    order_id INT NOT NULL,
                    product_name VARCHAR(100) NOT NULL,
                    quantity INT NOT NULL,
                    price DECIMAL(10,2) NOT NULL,
                    subtotal DECIMAL(10,2) NOT NULL,
                    FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE
                )',
                'CREATE INDEX idx_order_id ON order_items(order_id)',
                'CREATE INDEX idx_product_name ON order_items(product_name)',
            ];

            foreach ($queries as $query) {
                await(AsyncPDO::execute($query));
            }
        });
    }

    public static function setupSQLiteSchema(): PromiseInterface
    {
        return async(function () {
            $queries = [
                'DROP TABLE IF EXISTS order_items',
                'DROP TABLE IF EXISTS orders',
                'DROP TABLE IF EXISTS users',
                'CREATE TABLE users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    first_name TEXT NOT NULL,
                    last_name TEXT NOT NULL,
                    email TEXT UNIQUE NOT NULL,
                    city TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )',
                'CREATE INDEX idx_email ON users(email)',
                'CREATE INDEX idx_city ON users(city)',
                "CREATE TABLE orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    total REAL NOT NULL,
                    status TEXT DEFAULT 'pending',
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (user_id) REFERENCES users(id)
                )",
                'CREATE INDEX idx_user_id ON orders(user_id)',
                'CREATE INDEX idx_status ON orders(status)',
                'CREATE INDEX idx_created_at ON orders(created_at)',
                'CREATE TABLE order_items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_id INTEGER NOT NULL,
                    product_name TEXT NOT NULL,
                    quantity INTEGER NOT NULL,
                    price REAL NOT NULL,
                    subtotal REAL NOT NULL,
                    FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE
                )',
                'CREATE INDEX idx_order_id ON order_items(order_id)',
                'CREATE INDEX idx_product_name ON order_items(product_name)',
            ];

            foreach ($queries as $query) {
                await(AsyncPDO::execute($query));
            }
        });
    }

    public static function simulateUserRegistration(): PromiseInterface
    {
        return AsyncPDO::transaction(function ($pdo) {
            $userData = self::generateUser();

            $stmt = $pdo->prepare(
                'INSERT INTO users (first_name, last_name, email, city, created_at) 
             VALUES (?, ?, ?, ?, ?)'
            );
            $stmt->execute([
                $userData['first_name'],
                $userData['last_name'],
                $userData['email'],
                $userData['city'],
                $userData['created_at'],
            ]);

            return (int) $pdo->lastInsertId();
        });
    }

    public static function simulateOrderWorkflow(int $userId): PromiseInterface
    {
        return AsyncPDO::transaction(function ($pdo) use ($userId) {
            $stmt = $pdo->prepare('SELECT id FROM users WHERE id = ?');
            $stmt->execute([$userId]);
            if (! $stmt->fetch()) {
                throw new \Exception('User not found');
            }

            $orderData = self::generateOrderData($userId);

            $stmt = $pdo->prepare(
                'INSERT INTO orders (user_id, total, status, created_at) 
             VALUES (?, ?, ?, ?)'
            );
            $stmt->execute([
                $orderData['user_id'],
                $orderData['total'],
                $orderData['status'],
                $orderData['created_at'],
            ]);

            $orderId = (int) $pdo->lastInsertId();

            foreach ($orderData['items'] as $item) {
                $stmt = $pdo->prepare(
                    'INSERT INTO order_items (order_id, product_name, quantity, price, subtotal) 
                 VALUES (?, ?, ?, ?, ?)'
                );
                $stmt->execute([
                    $orderId,
                    $item['product_name'],
                    $item['quantity'],
                    $item['price'],
                    $item['subtotal'],
                ]);
            }

            return [
                'order_id' => $orderId,
                'total' => $orderData['total'],
                'items_count' => count($orderData['items']),
            ];
        });
    }

    public static function simulateAnalyticsQueries(): PromiseInterface
    {
        return async(function () {
            $queries = [
                AsyncPDO::query('
                    SELECT DATE(created_at) as date, 
                           COUNT(*) as orders_count,
                           SUM(total) as daily_revenue
                    FROM orders 
                    GROUP BY DATE(created_at)
                    ORDER BY date DESC
                    LIMIT 7
                '),
                AsyncPDO::query('
                    SELECT u.first_name, u.last_name, u.email, u.city,
                           COUNT(o.id) as order_count,
                           SUM(o.total) as total_spent
                    FROM users u
                    JOIN orders o ON u.id = o.user_id
                    GROUP BY u.id, u.first_name, u.last_name, u.email, u.city
                    ORDER BY total_spent DESC
                    LIMIT 10
                '),
                AsyncPDO::query('
                    SELECT status, COUNT(*) as count
                    FROM orders
                    GROUP BY status
                '),
            ];

            return await(Promise::all($queries));
        });
    }

    public static function simulateHeavyReadOperations(): PromiseInterface
    {
        return async(function () {
            $readOperations = [];

            for ($i = 0; $i < 3; $i++) {
                $readOperations[] = AsyncPDO::query('
                    SELECT u.*, COUNT(o.id) as order_count
                    FROM users u
                    LEFT JOIN orders o ON u.id = o.user_id
                    GROUP BY u.id, u.first_name, u.last_name, u.email, u.city, u.created_at
                    ORDER BY order_count DESC
                    LIMIT 20
                ');
            }

            return await(Promise::all($readOperations));
        });
    }

    public static function simulateInventoryUpdates()
    {
        return AsyncPDO::transaction(function ($pdo) {
            $statuses = ['processing', 'shipped', 'delivered'];
            $newStatus = $statuses[array_rand($statuses)];

            $stmt = $pdo->prepare("
            SELECT id FROM orders 
            WHERE status = 'pending' 
            LIMIT 5
            ");
            $stmt->execute();
            $ids = $stmt->fetchAll(\PDO::FETCH_COLUMN);

            if (empty($ids)) {
                return 0;
            }

            $placeholders = str_repeat('?,', count($ids) - 1) . '?';
            $stmt = $pdo->prepare("
            UPDATE orders 
            SET status = ? 
            WHERE id IN ($placeholders)
        ");

            $stmt->execute(array_merge([$newStatus], $ids));

            return $stmt->rowCount();
        });
    }

    public static function simulateUserSession(int $sessionId, int $operationsCount): PromiseInterface
    {
        return async(function () use ($sessionId, $operationsCount) {
            $results = [
                'session_id' => $sessionId,
                'operations' => [],
                'total_time' => 0,
                'errors' => 0,
            ];

            $sessionStart = microtime(true);
            $createdUsers = [];

            for ($op = 1; $op <= $operationsCount; $op++) {
                $operationStart = microtime(true);

                try {
                    $operation = rand(1, 100);

                    if ($operation <= 30) {
                        $userId = await(self::simulateUserRegistration());
                        $createdUsers[] = $userId;
                        $results['operations'][] = ['type' => 'user_registration', 'user_id' => $userId];
                    } elseif ($operation <= 65 && ! empty($createdUsers)) {
                        $userId = $createdUsers[array_rand($createdUsers)];
                        $orderResult = await(self::simulateOrderWorkflow($userId));
                        $results['operations'][] = ['type' => 'order_workflow', 'result' => $orderResult];
                    } elseif ($operation <= 80) {
                        await(self::simulateAnalyticsQueries());
                        $results['operations'][] = ['type' => 'analytics_queries'];
                    } elseif ($operation <= 95) {
                        await(self::simulateHeavyReadOperations());
                        $results['operations'][] = ['type' => 'heavy_reads'];
                    } else {
                        $updatedCount = await(self::simulateInventoryUpdates());
                        $results['operations'][] = ['type' => 'inventory_updates', 'updated' => $updatedCount];
                    }

                    $operationTime = (microtime(true) - $operationStart) * 1000;
                    $results['operations'][count($results['operations']) - 1]['time_ms'] = $operationTime;
                } catch (\Exception $e) {
                    $results['errors']++;
                    $results['operations'][] = [
                        'type' => 'error',
                        'message' => $e->getMessage(),
                        'time_ms' => (microtime(true) - $operationStart) * 1000,
                    ];
                }
            }

            $results['total_time'] = (microtime(true) - $sessionStart) * 1000;

            return $results;
        });
    }

    public static function runStressTest(int $concurrentUsers, int $operationsPerUser): PromiseInterface
    {
        return async(function () use ($concurrentUsers, $operationsPerUser) {
            $startTime = microtime(true);

            $sessionPromises = [];
            for ($i = 1; $i <= $concurrentUsers; $i++) {
                $sessionPromises[] = self::simulateUserSession($i, $operationsPerUser);
            }

            $sessionResults = await(Promise::all($sessionPromises));

            $endTime = microtime(true);

            $totalOperations = $concurrentUsers * $operationsPerUser;
            $totalTime = ($endTime - $startTime) * 1000;
            $totalErrors = array_sum(array_column($sessionResults, 'errors'));
            $successfulOperations = $totalOperations - $totalErrors;
            $avgSessionTime = array_sum(array_column($sessionResults, 'total_time')) / count($sessionResults);
            $operationsPerSecond = ($successfulOperations / ($totalTime / 1000));

            return [
                'total_time_ms' => $totalTime,
                'avg_session_time_ms' => $avgSessionTime,
                'operations_per_second' => $operationsPerSecond,
                'total_operations' => $totalOperations,
                'successful_operations' => $successfulOperations,
                'total_errors' => $totalErrors,
                'concurrent_users' => $concurrentUsers,
                'concurrency_ratio' => $avgSessionTime / $totalTime,
            ];
        });
    }
}
