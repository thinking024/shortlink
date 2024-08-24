/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.thinking024.shortlink.project.mq.idempotent;

import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * 消息队列幂等处理器

 */
@Component
@RequiredArgsConstructor
public class MessageQueueIdempotentHandler {

    private final StringRedisTemplate stringRedisTemplate;

    private static final String IDEMPOTENT_KEY_PREFIX = "short-link:idempotent:";

    /**
     * 判断当前消息是否已经开始处理了
     *
     * @param messageId 消息唯一标识
     * @return True，如果消息之前已开始处理
     */
    public boolean isMessageBeingConsumed(String messageId) {
        String key = IDEMPOTENT_KEY_PREFIX + messageId;
        // 消息id作为key放入redis，作为 预消费标识符号
        // 若放入前key不存在，则返回true，并设置value为“0”，代表 准备开始消费
        // 若存在，返回false，代表 之前已经有人消费过了
        // 设置2min过期时间，防止没完成消费，但是又没删标识的情况；同时避免长时间的key积压在内存中
        Boolean result = stringRedisTemplate.opsForValue().setIfAbsent(key, "0", 2, TimeUnit.MINUTES);
        return Boolean.FALSE.equals(result);
    }

    /**
     * 判断消息消费流程是否执行完成
     *
     * @param messageId 消息唯一标识
     * @return 消息是否执行完成
     */
    public boolean isAccomplish(String messageId) {
        String key = IDEMPOTENT_KEY_PREFIX + messageId;
        return Objects.equals(stringRedisTemplate.opsForValue().get(key), "1");
    }

    /**
     * 设置消息流程执行完成
     *
     * @param messageId 消息唯一标识
     */
    public void setAccomplish(String messageId) {
        String key = IDEMPOTENT_KEY_PREFIX + messageId;
        stringRedisTemplate.opsForValue().set(key, "1", 2, TimeUnit.MINUTES); // 将value设为“2”，代表已消费完
    }

    /**
     * 如果消息处理遇到异常情况，删除幂等标识
     *
     * @param messageId 消息唯一标识
     */
    public void delMessageProcessed(String messageId) {
        String key = IDEMPOTENT_KEY_PREFIX + messageId;
        stringRedisTemplate.delete(key);
    }
}
