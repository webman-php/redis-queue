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

class RedisConnection extends \Redis
{
    /**
     * @param $queue
     * @param $data
     * @param $delay
     * @return bool
     */
    public function send($queue, $data, $delay = 0)
    {
        $queue_waiting = '{redis-queue}-waiting';
        $queue_delay = '{redis-queue}-delayed';
        $now = time();
        $package_str = json_encode([
            'id'       => rand(),
            'time'     => $now,
            'delay'    => 0,
            'attempts' => 0,
            'queue'    => $queue,
            'data'     => $data
        ]);
        if ($delay) {
            return (bool)$this->zAdd($queue_delay, $now + $delay, $package_str);
        }
        return (bool)$this->lPush($queue_waiting.$queue, $package_str);
    }
}
