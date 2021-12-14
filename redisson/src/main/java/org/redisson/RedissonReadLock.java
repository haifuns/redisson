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
public class RedissonReadLock extends RedissonLock implements RLock {

    public RedissonReadLock(CommandAsyncExecutor commandExecutor, String name) {
        super(commandExecutor, name);
    }

    @Override
    String getChannelName() {
        return prefixName("redisson_rwlock", getRawName());
    }
    
    String getWriteLockName(long threadId) {
        return super.getLockName(threadId) + ":write";
    }

    String getReadWriteTimeoutNamePrefix(long threadId) {
        return suffixName(getRawName(), getLockName(threadId)) + ":rwlock_timeout";
    }
    
    @Override
    <T> RFuture<T> tryLockInnerAsync(long waitTime, long leaseTime, TimeUnit unit, long threadId, RedisStrictCommand<T> command) {
        return evalWriteAsync(getRawName(), LongCodec.INSTANCE, command,
                                // hash key=lock name 锁名，field=mode 锁模式
                                "local mode = redis.call('hget', KEYS[1], 'mode'); " +
                                // 如果模式为空，说明没有加过读写锁
                                "if (mode == false) then " +
                                  // 设置模式为读锁
                                  "redis.call('hset', KEYS[1], 'mode', 'read'); " +
                                  // 新增锁，hash key=lock name，field=uuid:threadId，value=1
                                  "redis.call('hset', KEYS[1], ARGV[2], 1); " +
                                  // 新增一个值 key={lock name}:uuid:threadId:rwlock_timeout:1，value=1
                                  "redis.call('set', KEYS[2] .. ':1', 1); " +
                                  // 设置过期时间，默认30秒
                                  "redis.call('pexpire', KEYS[2] .. ':1', ARGV[1]); " +
                                  "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                                  "return nil; " +
                                "end; " +
                                // 如果是读锁模式，或者是写锁并且field uuid:threadId:write值为1
                                "if (mode == 'read') or (mode == 'write' and redis.call('hexists', KEYS[1], ARGV[3]) == 1) then " +
                                  // 重入锁，field=uuid:threadId，值加1
                                  "local ind = redis.call('hincrby', KEYS[1], ARGV[2], 1); " + 
                                  // {lock name}:uuid:threadId:rwlock_timeout:锁重入次数
                                  "local key = KEYS[2] .. ':' .. ind;" +
                                  // todo ?
                                  "redis.call('set', key, 1); " +
                                  "redis.call('pexpire', key, ARGV[1]); " +
                                  "local remainTime = redis.call('pttl', KEYS[1]); " +
                                  // 更新过期时间
                                  "redis.call('pexpire', KEYS[1], math.max(remainTime, ARGV[1])); " +
                                  "return nil; " +
                                "end;" +
                                "return redis.call('pttl', KEYS[1]);",
                        // KEY[1] lock name，KEY[2] {lock name}:uuid:threadId:rwlock_timeout
                        Arrays.<Object>asList(getRawName(), getReadWriteTimeoutNamePrefix(threadId)),
                        // ARGV[1] 超时时间 默认30秒，ARGV[2] uuid:threadId，ARGV[3] uuid:threadId:write
                        unit.toMillis(leaseTime), getLockName(threadId), getWriteLockName(threadId));
    }

    @Override
    protected RFuture<Boolean> unlockInnerAsync(long threadId) {
        String timeoutPrefix = getReadWriteTimeoutNamePrefix(threadId);
        String keyPrefix = getKeyPrefix(threadId, timeoutPrefix);

        return evalWriteAsync(getRawName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                // 获取锁模式
                "local mode = redis.call('hget', KEYS[1], 'mode'); " +
                "if (mode == false) then " +
                    // 模式不存在说明锁不存在，直接发布解锁消息，返回成功
                    "redis.call('publish', KEYS[2], ARGV[1]); " +
                    "return 1; " +
                "end; " +
                // 获取当前线程持有锁重入次数
                "local lockExists = redis.call('hexists', KEYS[1], ARGV[2]); " +
                "if (lockExists == 0) then " +
                    "return nil;" +
                "end; " +
                    
                // 重入次数 - 1
                "local counter = redis.call('hincrby', KEYS[1], ARGV[2], -1); " + 
                "if (counter == 0) then " +
                    // 如果解锁前只重入了一次，说明可以解锁了，直接删除
                    "redis.call('hdel', KEYS[1], ARGV[2]); " + 
                "end;" +
                // 删除标记 {lock name}:uuid:threadId:rwlock_timeout:最后一次重入
                "redis.call('del', KEYS[3] .. ':' .. (counter+1)); " +
                
                // 如果hash中的字段数量大于1
                "if (redis.call('hlen', KEYS[1]) > 1) then " +
                    "local maxRemainTime = -3; " + 
                    // 获得hash key = lock name所有field
                    "local keys = redis.call('hkeys', KEYS[1]); " + 
                    "for n, key in ipairs(keys) do " + 
                        // 遍历field，todo，现在看就是在找field = uuid:threadId，得到重入次数
                        "counter = tonumber(redis.call('hget', KEYS[1], key)); " + 
                        "if type(counter) == 'number' then " + 
                            "for i=counter, 1, -1 do " + 
                                // 从1开始遍历到重入次数，得到所有{lock name}:uuid:threadId:rwlock_timeout:x的过期时间
                                "local remainTime = redis.call('pttl', KEYS[4] .. ':' .. key .. ':rwlock_timeout:' .. i); " + 
                                // 找到所有重入标记中ttl最大值
                                "maxRemainTime = math.max(remainTime, maxRemainTime);" + 
                            "end; " + 
                        "end; " + 
                    "end; " +
                            
                    // 如果所有重入标记最大超时时间大于0，说明锁还有重入次数
                    "if maxRemainTime > 0 then " +
                        // 就把过期时间设置给锁，todo 为啥？
                        "redis.call('pexpire', KEYS[1], maxRemainTime); " +
                        "return 0; " +
                    "end;" + 
                        
                    // 如果模式为写锁，直接返回失败
                    "if mode == 'write' then " + 
                        "return 0;" + 
                    "end; " +
                "end; " +
                    
                // 否则，说明可以解锁，直接删除锁，发布一条解锁消息
                "redis.call('del', KEYS[1]); " +
                "redis.call('publish', KEYS[2], ARGV[1]); " +
                "return 1; ",
                // key[1] lock name，key[2] redisson_rwlock:{lock name}，key[3] {lock name}:uuid:threadId:rwlock_timeout，key[4] {lock name}:uuid:threadId
                Arrays.<Object>asList(getRawName(), getChannelName(), timeoutPrefix, keyPrefix),
                // ARGV[1] 解锁消息通道，ARGV[2]，uuid:threadId
                LockPubSub.UNLOCK_MESSAGE, getLockName(threadId));
    }

    protected String getKeyPrefix(long threadId, String timeoutPrefix) {
        return timeoutPrefix.split(":" + getLockName(threadId))[0];
    }
    
    @Override
    protected RFuture<Boolean> renewExpirationAsync(long threadId) {
        String timeoutPrefix = getReadWriteTimeoutNamePrefix(threadId);
        String keyPrefix = getKeyPrefix(threadId, timeoutPrefix);
        
        return evalWriteAsync(getRawName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                "local counter = redis.call('hget', KEYS[1], ARGV[2]); " +
                "if (counter ~= false) then " +
                    "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                    
                    "if (redis.call('hlen', KEYS[1]) > 1) then " +
                        "local keys = redis.call('hkeys', KEYS[1]); " + 
                        "for n, key in ipairs(keys) do " + 
                            "counter = tonumber(redis.call('hget', KEYS[1], key)); " + 
                            "if type(counter) == 'number' then " + 
                                "for i=counter, 1, -1 do " + 
                                    "redis.call('pexpire', KEYS[2] .. ':' .. key .. ':rwlock_timeout:' .. i, ARGV[1]); " + 
                                "end; " + 
                            "end; " + 
                        "end; " +
                    "end; " +
                    
                    "return 1; " +
                "end; " +
                "return 0;",
            Arrays.<Object>asList(getRawName(), keyPrefix),
            internalLockLeaseTime, getLockName(threadId));
    }
    
    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }

    @Override
    public RFuture<Boolean> forceUnlockAsync() {
        cancelExpirationRenewal(null);
        return evalWriteAsync(getRawName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                "if (redis.call('hget', KEYS[1], 'mode') == 'read') then " +
                    "redis.call('del', KEYS[1]); " +
                    "redis.call('publish', KEYS[2], ARGV[1]); " +
                    "return 1; " +
                "end; " +
                "return 0; ",
                Arrays.<Object>asList(getRawName(), getChannelName()), LockPubSub.UNLOCK_MESSAGE);
    }

    @Override
    public boolean isLocked() {
        RFuture<String> future = commandExecutor.writeAsync(getRawName(), StringCodec.INSTANCE, RedisCommands.HGET, getRawName(), "mode");
        String res = get(future);
        return "read".equals(res);
    }

}
