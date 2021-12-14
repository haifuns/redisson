/**
 * Copyright (c) 2013-2021 Nikita Koksharov
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.redisson;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import org.redisson.api.RFuture;
import org.redisson.api.RLock;
import org.redisson.client.codec.LongCodec;
import org.redisson.client.codec.StringCodec;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.client.protocol.RedisStrictCommand;
import org.redisson.command.CommandAsyncExecutor;
import org.redisson.pubsub.LockPubSub;

/**
 * Lock will be removed automatically if client disconnects.
 *
 * @author Nikita Koksharov
 *
 */
public class RedissonWriteLock extends RedissonLock implements RLock {

    protected RedissonWriteLock(CommandAsyncExecutor commandExecutor, String name) {
        super(commandExecutor, name);
    }

    @Override
    String getChannelName() {
        return prefixName("redisson_rwlock", getRawName());
    }

    @Override
    protected String getLockName(long threadId) {
        return super.getLockName(threadId) + ":write";
    }
    
    @Override
    <T> RFuture<T> tryLockInnerAsync(long waitTime, long leaseTime, TimeUnit unit, long threadId, RedisStrictCommand<T> command) {
        return evalWriteAsync(getRawName(), LongCodec.INSTANCE, command,
                            // 写锁情况锁结构
                            // 1. 无锁加写锁，"lock1": {"mode": "write", "uuid1:thread1:write": 1}
                            // 2. 同一个线程重入加写锁，"lock1": {"mode": "write", "uuid1:thread1:write": 2}

                            // 获取锁的模式
                            "local mode = redis.call('hget', KEYS[1], 'mode'); " +
                            // 如果获取不到说明没加过锁，直接添加锁、设置写锁模式、设置过期时间
                            "if (mode == false) then " +
                                  "redis.call('hset', KEYS[1], 'mode', 'write'); " +
                                  // hash key = lock name，field = uuid:threadId:write
                                  "redis.call('hset', KEYS[1], ARGV[2], 1); " +
                                  "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                                  "return nil; " +
                              "end; " +
                              // 如果锁的模式是写锁
                              "if (mode == 'write') then " +
                                  // 如果写锁是当前线程加的，那么就把重入次数加1，更新过期时间
                                  "if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then " +
                                      "redis.call('hincrby', KEYS[1], ARGV[2], 1); " + 
                                      "local currentExpire = redis.call('pttl', KEYS[1]); " +
                                      "redis.call('pexpire', KEYS[1], currentExpire + ARGV[1]); " +
                                      "return nil; " +
                                  "end; " +
                                "end;" +
                                // 其他情况加锁失败，返回过期时间
                                "return redis.call('pttl', KEYS[1]);",
                        // KEY[1] lock name
                        Arrays.<Object>asList(getRawName()),
                        // ARGV[1] 过期时间，ARGV[2] uuid:threadId:write
                        unit.toMillis(leaseTime), getLockName(threadId));
    }

    @Override
    protected RFuture<Boolean> unlockInnerAsync(long threadId) {
        return evalWriteAsync(getRawName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                // 查询锁模式
                "local mode = redis.call('hget', KEYS[1], 'mode'); " +
                "if (mode == false) then " +
                    // 如果锁不存在，直接发布解锁消息
                    "redis.call('publish', KEYS[2], ARGV[1]); " +
                    "return 1; " +
                "end;" +
                // 如果模式是写锁
                "if (mode == 'write') then " +
                    // 查看当前线程加的写锁重入次数
                    "local lockExists = redis.call('hexists', KEYS[1], ARGV[3]); " +
                    // 如果是空，直接返回
                    "if (lockExists == 0) then " +
                        "return nil;" +
                    "else " +
                        // 如果写锁重入次数不是空，直接减1
                        "local counter = redis.call('hincrby', KEYS[1], ARGV[3], -1); " +
                        "if (counter > 0) then " +
                            // 如果可重入了多次，更新过期时间返回
                            "redis.call('pexpire', KEYS[1], ARGV[2]); " +
                            "return 0; " +
                        "else " +
                            // 如果只加了一次读锁，当前解锁写锁后还有可能存在读锁，需要把加锁模式转换为读锁
                            
                            // 直接删除写锁 
                            "redis.call('hdel', KEYS[1], ARGV[3]); " +
                            // 如果hash里只剩下field mode，直接删除锁
                            "if (redis.call('hlen', KEYS[1]) == 1) then " +
                                "redis.call('del', KEYS[1]); " +
                                "redis.call('publish', KEYS[2], ARGV[1]); " + 
                            "else " +
                                // 如果hash里field大于1，把锁模式修改为读锁
                                // has unlocked read-locks
                                "redis.call('hset', KEYS[1], 'mode', 'read'); " +
                            "end; " +
                            "return 1; "+
                        "end; " +
                    "end; " +
                "end; "
                + "return nil;",
                // KEY[1] lock name，KEY[2] redisson_rwlock:{lock name}
        Arrays.<Object>asList(getRawName(), getChannelName()),
        // ARGV[1] 解锁标记，ARGV[2] 过期时间，ARGV[3] uuid:thread:write
        LockPubSub.READ_UNLOCK_MESSAGE, internalLockLeaseTime, getLockName(threadId));
    }
    
    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }

    @Override
    public RFuture<Boolean> forceUnlockAsync() {
        cancelExpirationRenewal(null);
        return evalWriteAsync(getRawName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
              "if (redis.call('hget', KEYS[1], 'mode') == 'write') then " +
                  "redis.call('del', KEYS[1]); " +
                  "redis.call('publish', KEYS[2], ARGV[1]); " +
                  "return 1; " +
              "end; " +
              "return 0; ",
              Arrays.<Object>asList(getRawName(), getChannelName()), LockPubSub.READ_UNLOCK_MESSAGE);
    }

    @Override
    public boolean isLocked() {
        RFuture<String> future = commandExecutor.writeAsync(getRawName(), StringCodec.INSTANCE, RedisCommands.HGET, getRawName(), "mode");
        String res = get(future);
        return "write".equals(res);
    }

}
