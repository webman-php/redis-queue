<?php

/**
 * This file is part of webman.
 *
 * Licensed under The MIT License
 * For full copyright and license information, please see the MIT-LICENSE.txt
 * Redistributions of files must retain the above copyright notice.
 *
 * @author    walkor<walkor@workerman.net>
 * @copyright walkor<walkor@workerman.net>
 * @link      http://www.workerman.net/
 * @license   http://www.opensource.org/licenses/mit-license.php MIT License
 */

namespace Webman\RedisQueue\Process;

use support\Container;
use Webman\RedisQueue\Client;

/**
 * Class Consumer
 * @package process
 */
class Consumer
{
    /**
     * @var string
     */
    protected string $_consumerDir = '';

    /**
     * @var array
     */
    protected array $_consumers = [];

    /**
     * @var string[]
     */
    protected array $_connections = [];

    /**
     * Consumer constructor.
     *
     * @param  string  $consumer_dir
     */
    public function __construct(string $consumer_dir = '')
    {
        $this->_consumerDir = $consumer_dir;
    }

    public function onWorkerStart(): void
    {
        if (! is_dir($this->_consumerDir)) {
            echo "Consumer directory {$this->_consumerDir} not exists\r\n";
            return;
        }

        $iterator = new \RecursiveIteratorIterator(
            new \RecursiveDirectoryIterator(
                $this->_consumerDir,
                \FilesystemIterator::CURRENT_AS_FILEINFO|\FilesystemIterator::SKIP_DOTS|\FilesystemIterator::KEY_AS_PATHNAME
            )
        );

        // 对每一个 consumer 设置订阅
        /** @var \SplFileInfo $file */
        foreach ($iterator as $pathname => $file) {
            // 文件夹或非 PHP 文件
            if ($file->isDir() || $file->getExtension() !== 'php') {
                continue;
            }

            // 非 Consumer 子类
            $class = str_replace('/', "\\", substr(substr($pathname, strlen(base_path())), 0, -4));
            if (! is_a($class, 'Webman\RedisQueue\Consumer', true)) {
                continue;
            }

            $consumer = Container::get($class);
            if (! $queue = $consumer->queue) {
                echo "Consumer {$class} queue not exists\r\n";
                continue;
            }

            // 保存链接和队列信息
            $connection_name = $consumer->connection ?? 'default';
            $this->_connections[$connection_name] = true;
            $this->_consumers[$queue] = $consumer;

            $connection = Client::connection($connection_name);
            $connection->subscribe($queue, [$consumer, 'consume']);
            if (method_exists($connection, 'onConsumeFailure')) {
                $connection->onConsumeFailure(function ($exception, $package) {
                    $consumer = $this->_consumers[$package['queue']] ?? null;
                    if ($consumer && method_exists($consumer, 'onConsumeFailure')) {
                        return call_user_func([$consumer, 'onConsumeFailure'], $exception, $package);
                    }
                    return $package;
                });
            }
        }
    }

    public function onWorkerReload(): void
    {
        // 关闭所有订阅的连接
        foreach ($this->_connections as $name => $value) {
            Client::connection($name)->close();
        }
    }
}
