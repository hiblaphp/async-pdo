<?php

declare(strict_types=1);

use Hibla\AsyncPDO\AsyncPDOConnection;
use Hibla\AsyncPDO\Enums\IsolationLevel;
use Hibla\AsyncPDO\Exceptions\TransactionFailedException;

use function Hibla\await;

describe('Transaction API Integration', function () {
    describe('SQLite Transactions', function () {
        /** @var AsyncPDOConnection|null */
        $db = null;

        beforeEach(function () use (&$db) {
            $config = [
                'driver' => 'sqlite',
                'database' => ':memory:',
            ];

            $db = new AsyncPDOConnection($config, 2);

            await($db->execute(
                'CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, email TEXT)'
            ));
        });

        afterEach(function () use (&$db) {
            if ($db !== null) {
                $db->reset();
                $db = null;
            }
        });

        it('commits transaction successfully', function () use (&$db) {
            $result = await($db->transaction(function ($tx) {
                $tx->execute('INSERT INTO users (name, email) VALUES (?, ?)', ['John', 'john@example.com']);
                $tx->execute('INSERT INTO users (name, email) VALUES (?, ?)', ['Jane', 'jane@example.com']);

                return $tx->query('SELECT COUNT(*) as count FROM users');
            }));

            expect($result[0]['count'])->toBe(2);

            $users = await($db->query('SELECT * FROM users ORDER BY id'));
            expect($users)->toHaveCount(2);
            expect($users[0]['name'])->toBe('John');
            expect($users[1]['name'])->toBe('Jane');
        });

        it('rolls back transaction on error', function () use (&$db) {
            try {
                await($db->transaction(function ($tx) {
                    $tx->execute('INSERT INTO users (name, email) VALUES (?, ?)', ['John', 'john@example.com']);

                    throw new Exception('Simulated error');
                }));
            } catch (TransactionFailedException $e) {
                // Expected
            }

            $count = await($db->fetchValue('SELECT COUNT(*) FROM users'));
            expect($count)->toBe(0);
        });

        it('executes onCommit callbacks', function () use (&$db) {
            $commitCalled = false;

            await($db->transaction(function ($tx) use (&$commitCalled) {
                $tx->onCommit(function () use (&$commitCalled) {
                    $commitCalled = true;
                });
                $tx->execute('INSERT INTO users (name, email) VALUES (?, ?)', ['John', 'john@example.com']);
            }));

            expect($commitCalled)->toBeTrue();
        });

        it('executes onRollback callbacks', function () use (&$db) {
            $rollbackCalled = false;

            try {
                await($db->transaction(function ($tx) use (&$rollbackCalled) {
                    $tx->onRollback(function () use (&$rollbackCalled) {
                        $rollbackCalled = true;
                    });
                    $tx->execute('INSERT INTO users (name, email) VALUES (?, ?)', ['John', 'john@example.com']);

                    throw new Exception('Force rollback');
                }));
            } catch (TransactionFailedException $e) {
                // Expected
            }

            expect($rollbackCalled)->toBeTrue();
        });

        it('retries transaction on failure', function () use (&$db) {
            $attempts = 0;

            await($db->transaction(function ($tx) use (&$attempts) {
                $attempts++;
                if ($attempts < 3) {
                    throw new Exception('Temporary failure');
                }
                $tx->execute('INSERT INTO users (name, email) VALUES (?, ?)', ['John', 'john@example.com']);
            }, 3));

            expect($attempts)->toBe(3);
            $count = await($db->fetchValue('SELECT COUNT(*) FROM users'));
            expect($count)->toBe(1);
        });

        it('uses Transaction query methods', function () use (&$db) {
            await($db->transaction(function ($tx) {
                $tx->execute('INSERT INTO users (name, email) VALUES (?, ?)', ['John', 'john@example.com']);
                $user = $tx->fetchOne('SELECT * FROM users WHERE name = ?', ['John']);
                expect($user['name'])->toBe('John');

                $count = $tx->fetchValue('SELECT COUNT(*) FROM users');
                expect($count)->toBe(1);

                $tx->execute('INSERT INTO users (name, email) VALUES (?, ?)', ['Jane', 'jane@example.com']);
                $users = $tx->query('SELECT * FROM users ORDER BY id');
                expect($users)->toHaveCount(2);
            }));
        });

        it('provides access to PDO connection', function () use (&$db) {
            await($db->transaction(function ($tx) {
                $pdo = $tx->getConnection();
                expect($pdo)->toBeInstanceOf(PDO::class);
                expect($pdo->inTransaction())->toBeTrue();
            }));
        });

        it('supports nested onCommit callbacks', function () use (&$db) {
            $callback1Called = false;
            $callback2Called = false;

            await($db->transaction(function ($tx) use (&$callback1Called, &$callback2Called) {
                $tx->onCommit(function () use (&$callback1Called) {
                    $callback1Called = true;
                });
                $tx->onCommit(function () use (&$callback2Called) {
                    $callback2Called = true;
                });
                $tx->execute('INSERT INTO users (name, email) VALUES (?, ?)', ['John', 'john@example.com']);
            }));

            expect($callback1Called)->toBeTrue();
            expect($callback2Called)->toBeTrue();
        });
    });

    describe('MySQL Transactions', function () {
        /** @var AsyncPDOConnection|null */
        $db = null;

        beforeEach(function () use (&$db) {
            if (empty(getenv('MYSQL_HOST'))) {
                test()->markTestSkipped('MySQL not configured');
            }

            $host = getenv('MYSQL_HOST') ?: 'localhost';
            if ($host === '127.0.0.1' && ! getenv('CI')) {
                $host = 'localhost';
            }

            $config = [
                'driver' => 'mysql',
                'host' => $host,
                'port' => (int) (getenv('MYSQL_PORT') ?: 3306),
                'database' => getenv('MYSQL_DATABASE') ?: 'test',
                'username' => getenv('MYSQL_USERNAME') ?: 'root',
                'password' => getenv('MYSQL_PASSWORD') ?: '',
                'charset' => 'utf8mb4',
            ];

            $db = new AsyncPDOConnection($config, 2);

            await($db->execute('DROP TABLE IF EXISTS transaction_test'));
            await($db->execute(
                'CREATE TABLE transaction_test (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(255),
                    balance DECIMAL(10,2),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )'
            ));
        });

        afterEach(function () use (&$db) {
            if ($db !== null) {
                await($db->execute('DROP TABLE IF EXISTS transaction_test'));
                $db->reset();
                $db = null;
            }
        });

        it('commits transaction successfully', function () use (&$db) {
            $result = await($db->transaction(function ($tx) {
                $tx->execute(
                    'INSERT INTO transaction_test (name, balance) VALUES (?, ?)',
                    ['Account A', 1000.00]
                );
                $tx->execute(
                    'INSERT INTO transaction_test (name, balance) VALUES (?, ?)',
                    ['Account B', 2000.00]
                );

                return $tx->query('SELECT COUNT(*) as count FROM transaction_test');
            }));

            expect($result[0]['count'])->toBe(2);

            $accounts = await($db->query('SELECT * FROM transaction_test ORDER BY id'));
            expect($accounts)->toHaveCount(2);
            expect($accounts[0]['name'])->toBe('Account A');
            expect((float) $accounts[0]['balance'])->toBe(1000.00);
        });

        it('rolls back transaction on error', function () use (&$db) {
            try {
                await($db->transaction(function ($tx) {
                    $tx->execute(
                        'INSERT INTO transaction_test (name, balance) VALUES (?, ?)',
                        ['Account A', 1000.00]
                    );

                    throw new Exception('Simulated error');
                }));
            } catch (TransactionFailedException $e) {
                // Expected
            }

            $count = await($db->fetchValue('SELECT COUNT(*) FROM transaction_test'));
            expect($count)->toBe(0);
        });

        it('handles money transfer with rollback', function () use (&$db) {
            await($db->execute(
                'INSERT INTO transaction_test (name, balance) VALUES (?, ?)',
                ['Account A', 1000.00]
            ));
            await($db->execute(
                'INSERT INTO transaction_test (name, balance) VALUES (?, ?)',
                ['Account B', 500.00]
            ));

            try {
                await($db->transaction(function ($tx) {
                    $accountA = $tx->fetchOne(
                        'SELECT * FROM transaction_test WHERE name = ?',
                        ['Account A']
                    );

                    $transferAmount = 1500.00;
                    if ($accountA['balance'] < $transferAmount) {
                        throw new Exception('Insufficient funds');
                    }

                    $tx->execute(
                        'UPDATE transaction_test SET balance = balance - ? WHERE name = ?',
                        [$transferAmount, 'Account A']
                    );
                    $tx->execute(
                        'UPDATE transaction_test SET balance = balance + ? WHERE name = ?',
                        [$transferAmount, 'Account B']
                    );
                }));
            } catch (TransactionFailedException $e) {
                // Expected
            }

            $accountA = await($db->fetchOne(
                'SELECT balance FROM transaction_test WHERE name = ?',
                ['Account A']
            ));
            $accountB = await($db->fetchOne(
                'SELECT balance FROM transaction_test WHERE name = ?',
                ['Account B']
            ));

            expect((float) $accountA['balance'])->toBe(1000.00);
            expect((float) $accountB['balance'])->toBe(500.00);
        });

        it('handles money transfer with commit', function () use (&$db) {
            await($db->execute(
                'INSERT INTO transaction_test (name, balance) VALUES (?, ?)',
                ['Account A', 1000.00]
            ));
            await($db->execute(
                'INSERT INTO transaction_test (name, balance) VALUES (?, ?)',
                ['Account B', 500.00]
            ));

            await($db->transaction(function ($tx) {
                $transferAmount = 300.00;

                $tx->execute(
                    'UPDATE transaction_test SET balance = balance - ? WHERE name = ?',
                    [$transferAmount, 'Account A']
                );
                $tx->execute(
                    'UPDATE transaction_test SET balance = balance + ? WHERE name = ?',
                    [$transferAmount, 'Account B']
                );
            }));

            $accountA = await($db->fetchOne(
                'SELECT balance FROM transaction_test WHERE name = ?',
                ['Account A']
            ));
            $accountB = await($db->fetchOne(
                'SELECT balance FROM transaction_test WHERE name = ?',
                ['Account B']
            ));

            expect((float) $accountA['balance'])->toBe(700.00);
            expect((float) $accountB['balance'])->toBe(800.00);
        });

        it('retries transaction with exponential backoff', function () use (&$db) {
            $attempts = 0;

            await($db->transaction(function ($tx) use (&$attempts) {
                $attempts++;
                if ($attempts < 2) {
                    throw new Exception('Deadlock detected');
                }
                $tx->execute(
                    'INSERT INTO transaction_test (name, balance) VALUES (?, ?)',
                    ['Retry Test', 100.00]
                );
            }, 3));

            expect($attempts)->toBe(2);
            $count = await($db->fetchValue('SELECT COUNT(*) FROM transaction_test'));
            expect($count)->toBe(1);
        });

        it('supports REPEATABLE READ isolation level', function () use (&$db) {
            await($db->execute(
                'INSERT INTO transaction_test (name, balance) VALUES (?, ?)',
                ['Test Account', 1000.00]
            ));

            await($db->transaction(function ($tx) {
                $account1 = $tx->fetchOne(
                    'SELECT balance FROM transaction_test WHERE name = ?',
                    ['Test Account']
                );

                expect((float) $account1['balance'])->toBe(1000.00);

                $account2 = $tx->fetchOne(
                    'SELECT balance FROM transaction_test WHERE name = ?',
                    ['Test Account']
                );

                expect((float) $account2['balance'])->toBe(1000.00);
            }, 1, IsolationLevel::REPEATABLE_READ));
        });

        it('supports SERIALIZABLE isolation level', function () use (&$db) {
            await($db->execute(
                'INSERT INTO transaction_test (name, balance) VALUES (?, ?)',
                ['Test Account', 1000.00]
            ));

            await($db->transaction(function ($tx) {
                $account = $tx->fetchOne(
                    'SELECT balance FROM transaction_test WHERE name = ?',
                    ['Test Account']
                );

                $tx->execute(
                    'UPDATE transaction_test SET balance = ? WHERE name = ?',
                    [1500.00, 'Test Account']
                );

                $updated = $tx->fetchOne(
                    'SELECT balance FROM transaction_test WHERE name = ?',
                    ['Test Account']
                );

                expect((float) $updated['balance'])->toBe(1500.00);
            }, 1, IsolationLevel::SERIALIZABLE));

            $finalBalance = await($db->fetchValue(
                'SELECT balance FROM transaction_test WHERE name = ?',
                ['Test Account']
            ));
            expect((float) $finalBalance)->toBe(1500.00);
        });
    });

    describe('PostgreSQL Transactions', function () {
        /** @var AsyncPDOConnection|null */
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
                'password' => getenv('PGSQL_PASSWORD') ?: 'postgres',
            ];

            $db = new AsyncPDOConnection($config, 2);

            // Setup test table
            await($db->execute('DROP TABLE IF EXISTS transaction_test'));
            await($db->execute(
                'CREATE TABLE transaction_test (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255),
                balance DECIMAL(10,2),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )'
            ));
        });

        afterEach(function () use (&$db) {
            if ($db !== null) {
                await($db->execute('DROP TABLE IF EXISTS transaction_test'));
                $db->reset();
                $db = null;
            }
        });

        it('commits transaction successfully', function () use (&$db) {
            $result = await($db->transaction(function ($tx) {
                $tx->execute(
                    'INSERT INTO transaction_test (name, balance) VALUES (?, ?)',
                    ['Account A', 1000.00]
                );
                $tx->execute(
                    'INSERT INTO transaction_test (name, balance) VALUES (?, ?)',
                    ['Account B', 2000.00]
                );

                return $tx->query('SELECT COUNT(*) as count FROM transaction_test');
            }));

            expect((int) $result[0]['count'])->toBe(2);
        });

        it('rolls back transaction on error', function () use (&$db) {
            try {
                await($db->transaction(function ($tx) {
                    $tx->execute(
                        'INSERT INTO transaction_test (name, balance) VALUES (?, ?)',
                        ['Account A', 1000.00]
                    );

                    throw new Exception('Simulated error');
                }));
            } catch (TransactionFailedException $e) {
                // Expected
            }

            $count = await($db->fetchValue('SELECT COUNT(*) FROM transaction_test'));
            expect((int) $count)->toBe(0);
        });

        it('handles concurrent updates', function () use (&$db) {
            await($db->execute(
                'INSERT INTO transaction_test (name, balance) VALUES (?, ?)',
                ['Shared Account', 1000.00]
            ));

            await($db->transaction(function ($tx) {
                $account = $tx->fetchOne(
                    'SELECT * FROM transaction_test WHERE name = ? FOR UPDATE',
                    ['Shared Account']
                );

                $newBalance = (float) $account['balance'] + 500.00;
                $tx->execute(
                    'UPDATE transaction_test SET balance = ? WHERE name = ?',
                    [$newBalance, 'Shared Account']
                );
            }));

            $finalBalance = await($db->fetchValue(
                'SELECT balance FROM transaction_test WHERE name = ?',
                ['Shared Account']
            ));
            expect((float) $finalBalance)->toBe(1500.00);
        });

        it('supports READ COMMITTED isolation level', function () use (&$db) {
            await($db->execute(
                'INSERT INTO transaction_test (name, balance) VALUES (?, ?)',
                ['Test Account', 1000.00]
            ));

            await($db->transaction(function ($tx) {
                $account = $tx->fetchOne(
                    'SELECT balance FROM transaction_test WHERE name = ?',
                    ['Test Account']
                );

                expect((float) $account['balance'])->toBe(1000.00);
            }, 1, IsolationLevel::READ_COMMITTED));
        });

        it('handles money transfer between accounts', function () use (&$db) {
            // Setup initial accounts
            await($db->execute(
                'INSERT INTO transaction_test (name, balance) VALUES (?, ?)',
                ['Account A', 1000.00]
            ));
            await($db->execute(
                'INSERT INTO transaction_test (name, balance) VALUES (?, ?)',
                ['Account B', 500.00]
            ));

            await($db->transaction(function ($tx) {
                $transferAmount = 300.00;

                $tx->execute(
                    'UPDATE transaction_test SET balance = balance - ? WHERE name = ?',
                    [$transferAmount, 'Account A']
                );
                $tx->execute(
                    'UPDATE transaction_test SET balance = balance + ? WHERE name = ?',
                    [$transferAmount, 'Account B']
                );
            }));

            $accountA = await($db->fetchOne(
                'SELECT balance FROM transaction_test WHERE name = ?',
                ['Account A']
            ));
            $accountB = await($db->fetchOne(
                'SELECT balance FROM transaction_test WHERE name = ?',
                ['Account B']
            ));

            expect((float) $accountA['balance'])->toBe(700.00);
            expect((float) $accountB['balance'])->toBe(800.00);
        });

        it('retries transaction on failure', function () use (&$db) {
            $attempts = 0;

            await($db->transaction(function ($tx) use (&$attempts) {
                $attempts++;
                if ($attempts < 2) {
                    throw new Exception('Temporary error');
                }
                $tx->execute(
                    'INSERT INTO transaction_test (name, balance) VALUES (?, ?)',
                    ['Retry Test', 100.00]
                );
            }, 3));

            expect($attempts)->toBe(2);
            $count = await($db->fetchValue('SELECT COUNT(*) FROM transaction_test'));
            expect((int) $count)->toBe(1);
        });

        it('executes onCommit and onRollback callbacks', function () use (&$db) {
            $commitCalled = false;

            await($db->transaction(function ($tx) use (&$commitCalled) {
                $tx->onCommit(function () use (&$commitCalled) {
                    $commitCalled = true;
                });
                $tx->execute(
                    'INSERT INTO transaction_test (name, balance) VALUES (?, ?)',
                    ['Test', 100.00]
                );
            }));

            expect($commitCalled)->toBeTrue();

            $rollbackCalled = false;

            try {
                await($db->transaction(function ($tx) use (&$rollbackCalled) {
                    $tx->onRollback(function () use (&$rollbackCalled) {
                        $rollbackCalled = true;
                    });
                    $tx->execute(
                        'INSERT INTO transaction_test (name, balance) VALUES (?, ?)',
                        ['Test2', 200.00]
                    );

                    throw new Exception('Force rollback');
                }));
            } catch (TransactionFailedException $e) {
                // Expected
            }

            expect($rollbackCalled)->toBeTrue();
        });
    });

    describe('SQL Server Transactions', function () {
        /** @var AsyncPDOConnection|null */
        $db = null;

        beforeEach(function () use (&$db) {
            if (getenv('CI')) {
                test()->markTestSkipped('SQL Server tests skipped in CI environment');
            }

            skipIfPhp84OrHigher();

            if (empty(getenv('MSSQL_HOST'))) {
                test()->markTestSkipped('SQL Server not configured');
            }

            $config = [
                'driver' => 'sqlsrv',
                'host' => getenv('MSSQL_HOST') ?: 'localhost',
                'port' => (int) (getenv('MSSQL_PORT') ?: 1433),
                'database' => getenv('MSSQL_DATABASE') ?: 'master',
                'username' => getenv('MSSQL_USERNAME') ?: 'sa',
                'password' => getenv('MSSQL_PASSWORD') ?: 'YourStrong@Passw0rd',
            ];

            $db = new AsyncPDOConnection($config, 2);

            // Setup test table
            await($db->execute('IF OBJECT_ID(\'dbo.transaction_test\', \'U\') IS NOT NULL DROP TABLE dbo.transaction_test'));
            await($db->execute(
                'CREATE TABLE transaction_test (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    name NVARCHAR(255),
                    balance DECIMAL(10,2),
                    created_at DATETIME DEFAULT GETDATE()
                )'
            ));
        });

        afterEach(function () use (&$db) {
            if ($db !== null) {
                await($db->execute('IF OBJECT_ID(\'dbo.transaction_test\', \'U\') IS NOT NULL DROP TABLE dbo.transaction_test'));
                $db->reset();
                $db = null;
            }
        });

        it('commits transaction successfully', function () use (&$db) {
            $result = await($db->transaction(function ($tx) {
                $tx->execute(
                    'INSERT INTO transaction_test (name, balance) VALUES (?, ?)',
                    ['Account A', 1000.00]
                );
                $tx->execute(
                    'INSERT INTO transaction_test (name, balance) VALUES (?, ?)',
                    ['Account B', 2000.00]
                );

                return $tx->query('SELECT COUNT(*) as count FROM transaction_test');
            }));

            expect((int) $result[0]['count'])->toBe(2);
        });

        it('rolls back transaction on error', function () use (&$db) {
            try {
                await($db->transaction(function ($tx) {
                    $tx->execute(
                        'INSERT INTO transaction_test (name, balance) VALUES (?, ?)',
                        ['Account A', 1000.00]
                    );

                    throw new Exception('Simulated error');
                }));
            } catch (TransactionFailedException $e) {
                // Expected
            }

            $count = await($db->fetchValue('SELECT COUNT(*) FROM transaction_test'));
            expect((int) $count)->toBe(0);
        });

        it('handles isolation levels', function () use (&$db) {
            await($db->execute(
                'INSERT INTO transaction_test (name, balance) VALUES (?, ?)',
                ['Test Account', 1000.00]
            ));

            await($db->transaction(function ($tx) {
                $account = $tx->fetchOne(
                    'SELECT balance FROM transaction_test WHERE name = ?',
                    ['Test Account']
                );

                expect((float) $account['balance'])->toBe(1000.00);
            }, 1, IsolationLevel::READ_COMMITTED));
        });
    });
});
