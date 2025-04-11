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
namespace Webman\RedisQueue;

use support\Log;
use Workerman\RedisQueue\Client as RedisClient;
use Workerman\RedisQueue\UnretryableException;
use Workerman\Timer;

/**
 * Class RedisQueue
 * @package support
 *
 * Strings methods
 * @method static void send($queue, $data, $delay=0)
 */
class Client extends RedisClient
{
    protected string $name = '';

    protected static int $retry_timer = 0;

    /**
     * @var Client[]
     */
    protected static array $_connections = [];

    /**
     * Get redis connection
     *
     * @param  string  $name
     * @return static
     */
    public static function connection(string $name = 'default'): static
    {
        if (! isset(static::$_connections[$name])) {
            $config = config('redis_queue', config('plugin.webman.redis-queue.redis', []));
            if (! isset($config[$name])) {
                throw new \RuntimeException("RedisQueue connection {$name} not found");
            }

            $client = new static($config[$name]['host'], $config[$name]['options']);
            if (method_exists($client, 'logger')) {
                $client->logger(Log::channel('plugin.webman.redis-queue.default'));
            }

            $client->name = $name;
            static::$_connections[$name] = $client;
        }
        return static::$_connections[$name];
    }

    public function close()
    {
        unset(static::$_connections[$this->name]);

        // 停止同步异步任务的定时器
        Timer::del(static::$retry_timer);

        // 将订阅队列清空，避免后续还有 pull 定时任务被添加
        $this->_subscribeQueues = [];

        // 延时 1.01s 关闭连接，避免关闭之前还有监听的数据到来(pull 的时候定时器延时为 0.000001，阻塞延时为 1，所以要稍大一些，确保最后一次 pull 完成)
        Timer::add(1.01, function () {
            $this->_redisSubscribe->close();
            $this->_redisSend->close();
        }, [], false);
    }

    public function pull()
    {
        $this->tryToPullDelayQueue();
        if (! $this->_subscribeQueues || ! empty($this->_redisSubscribe->brPoping)) {
            return;
        }

        $cb = function ($data) use (&$cb) {
            // 消费数据
            $this->consume($data);

            // 重新监听
            if ($this->_subscribeQueues) {
                $this->_redisSubscribe->brPoping = 1;
                Timer::add(0.000001, [$this->_redisSubscribe, 'brPop'], [\array_keys($this->_subscribeQueues), 1, $cb], false);
            }

            // 处理信号
            function_exists('pcntl_signal_dispatch') && pcntl_signal_dispatch();
        };

        $this->_redisSubscribe->brPoping = 1;
        $this->_redisSubscribe->brPop(\array_keys($this->_subscribeQueues), 1, $cb);
    }

    protected function tryToPullDelayQueue()
    {
        if (static::$retry_timer) {
            return;
        }
        static::$retry_timer = Timer::add(1, function () {
            $now = time();
            $options = ['LIMIT', 0, 128];
            $this->_redisSend->zrevrangebyscore($this->_options['prefix'] . static::QUEUE_DELAYED, $now, '-inf', $options, function ($items) {
                if ($items === false) {
                    throw new \RuntimeException($this->_redisSend->error());
                }
                foreach ($items as $package_str) {
                    $this->_redisSend->zRem($this->_options['prefix'] . static::QUEUE_DELAYED, $package_str, function ($result) use ($package_str) {
                        if ($result !== 1) {
                            return;
                        }
                        $package = \json_decode($package_str, true);
                        if (!$package) {
                            $this->_redisSend->lPush($this->_options['prefix'] . static::QUEUE_FAILED, $package_str);
                            return;
                        }
                        $this->_redisSend->lPush($this->_options['prefix'] . static::QUEUE_WAITING . $package['queue'], $package_str);
                    });
                }
            });
        });
    }

    protected function consume($data)
    {
        if (empty($data)) {
            return;
        }

        $this->_redisSubscribe->brPoping = 0;

        [$redis_key, $package_str] = $data;
        if (! $package = json_decode($package_str, true)) {
            $this->_redisSend->lPush($this->_options['prefix'] . static::QUEUE_FAILED, $package_str);
            return;
        }

        // 取消订阅/未订阅，放回队列
        if (! $callback = $this->_subscribeQueues[$redis_key] ?? null) {
            $this->_redisSend->rPush($redis_key, $package_str);
            return;
        }

        try {
            \call_user_func($callback, $package['data']);
        } catch (\Throwable $e) {
            $this->log((string)$e);
            $package['max_attempts'] = $this->_options['max_attempts'];
            $package['error'] = $e->getMessage();

            if ($e instanceof UnretryableException) {
                $this->fail($package);
                return;
            }

            $modified = null;
            if ($this->_consumeFailure) {
                try {
                    $modified = \call_user_func($this->_consumeFailure, $e, $package);
                } catch (\Throwable $ta) {
                    $this->log((string)$ta);
                }
            }

            if (is_array($modified)) {
                $package['data'] = $modified['data'] ?? $package['data'];
                $package['attempts'] = $modified['attempts'] ?? $package['attempts'];
                $package['max_attempts'] = $modified['max_attempts'] ?? $package['max_attempts'];
                $package['error'] = $modified['error'] ?? $package['error'];
            }

            if (++$package['attempts'] > $package['max_attempts']) {
                $this->fail($package);
            } else {
                $this->retry($package);
            }
        }
    }

    /**
     * @param $name
     * @param $arguments
     * @return mixed
     */
    public static function __callStatic($name, $arguments)
    {
        return static::connection()->{$name}(... $arguments);
    }
}
