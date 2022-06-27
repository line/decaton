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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import com.linecorp.decaton.client.DecatonClient;
import com.linecorp.decaton.processor.runtime.DynamicProperty;
import com.linecorp.decaton.processor.runtime.ProcessorProperties;
import com.linecorp.decaton.processor.runtime.ProcessorSubscription;
import com.linecorp.decaton.processor.runtime.ProcessorsBuilder;
import com.linecorp.decaton.processor.runtime.StaticPropertySupplier;
import com.linecorp.decaton.processor.internal.HashableByteArray;
import com.linecorp.decaton.protobuf.ProtocolBuffersDeserializer;
import com.linecorp.decaton.protocol.Sample.HelloTask;
import com.linecorp.decaton.testing.KafkaClusterRule;
import com.linecorp.decaton.testing.TestUtils;

public class RateLimiterTest {
    @ClassRule
    public static KafkaClusterRule rule = new KafkaClusterRule();

    private String topicName;

    @Before
    public void setUp() {
        topicName = rule.admin().createRandomTopic(3, 3);
    }

    @After
    public void tearDown() {
        rule.admin().deleteTopics(true, topicName);
    }

    @Test(timeout = 30000)
    public void testPropertyDynamicSwitch() throws Exception {
        Set<String> keys = new HashSet<>();

        for (int i = 0; i < 10000; i++) {
            keys.add("key" + i);
        }
        Set<HashableByteArray> processedKeys = Collections.synchronizedSet(new HashSet<>());
        CountDownLatch processLatch = new CountDownLatch(keys.size());

        DecatonProcessor<HelloTask> processor = (context, task) -> {
            processedKeys.add(new HashableByteArray(context.key()));
            processLatch.countDown();
        };

        DynamicProperty<Long> rateProp = new DynamicProperty<>(ProcessorProperties.CONFIG_PROCESSING_RATE);
        try (ProcessorSubscription subscription = TestUtils.subscription(
                rule.bootstrapServers(),
                builder -> builder.processorsBuilder(ProcessorsBuilder
                                                             .consuming(topicName,
                                                                        new ProtocolBuffersDeserializer<>(HelloTask.parser()))
                                                             .thenProcess(processor))
                                  .addProperties(StaticPropertySupplier.of(rateProp)));
             DecatonClient<HelloTask> client = TestUtils.client(topicName, rule.bootstrapServers())) {

            int count = 0;
            for (String key : keys) {
                if (++count % 1000 == 0) {
                    rateProp.set((long) count / 10);
                }
                client.put(key, HelloTask.getDefaultInstance());
            }
            processLatch.await();
        }

        assertEquals(10000, processedKeys.size());
    }
}
