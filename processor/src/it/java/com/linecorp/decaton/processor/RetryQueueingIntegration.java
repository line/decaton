/*
 * Copyright 2020 LINE Corporation
 *
 * LINE Corporation licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.linecorp.decaton.processor;

import static org.junit.Assert.assertEquals;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import org.junit.ClassRule;
import org.junit.Test;

import com.linecorp.decaton.client.DecatonClient;
import com.linecorp.decaton.processor.runtime.ProcessorSubscription;
import com.linecorp.decaton.processor.runtime.RetryConfig;
import com.linecorp.decaton.protobuf.ProtocolBuffersDeserializer;
import com.linecorp.decaton.protocol.Sample.HelloTask;
import com.linecorp.decaton.testing.KafkaClusterRule;

public class RetryQueueingIntegration {
    @ClassRule
    public static KafkaClusterRule rule = new KafkaClusterRule();

    @Test(timeout = 30000)
    public void testRetryQueuing() throws Exception {
        String topicName = "test-" + UUID.randomUUID();
        rule.admin().createTopic(topicName, 3, 3);
        rule.admin().createTopic(topicName + "-retry", 3, 3);

        Set<String> keys = new HashSet<>();
        Set<String> keysNeedRetry = new HashSet<>();

        // scenario:
        //   * produce 100 tasks at total.
        //   * half tasks have "-needRetry" suffix. they will be retried once, and processed after retried
        //   * other tasks will be processed as usual
        for (int i = 0; i < 50; i++) {
            keysNeedRetry.add("key" + i + "-needRetry");
        }
        for (int i = 50; i < 100; i++) {
            keys.add("key" + i);
        }
        Set<String> processedKeys = Collections.synchronizedSet(new HashSet<>());
        Set<String> retriedKeys = Collections.synchronizedSet(new HashSet<>());
        CountDownLatch processLatch = new CountDownLatch(keys.size() + keysNeedRetry.size());

        DecatonProcessor<HelloTask> processor = (context, task) -> {
            String key = context.key();
            if (!key.endsWith("needRetry")) {
                processedKeys.add(key);
                processLatch.countDown();
                return;
            }
            if (context.metadata().retryCount() == 0) {
                context.retry();
            } else {
                retriedKeys.add(key);
                processLatch.countDown();
            }
        };

        try (ProcessorSubscription subscription = TestUtils.subscription(
                rule.bootstrapServers(),
                ProcessorsBuilder.consuming(topicName, new ProtocolBuffersDeserializer<>(HelloTask.parser()))
                                 .thenProcess(processor),
                RetryConfig.withBackoff(Duration.ofMillis(10)));
             DecatonClient<HelloTask> client = TestUtils.client(topicName, rule.bootstrapServers())) {

            keys.forEach(key -> client.put(key, HelloTask.getDefaultInstance()));
            keysNeedRetry.forEach(key -> client.put(key, HelloTask.getDefaultInstance()));

            processLatch.await();

            assertEquals(keys, processedKeys);
            assertEquals(keysNeedRetry, retriedKeys);
        }
    }
}
