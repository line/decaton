/*
 * Copyright 2023 LINE Corporation
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

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import com.linecorp.decaton.client.DecatonClient;
import com.linecorp.decaton.processor.runtime.PerKeyQuotaConfig;
import com.linecorp.decaton.processor.runtime.ProcessorProperties;
import com.linecorp.decaton.processor.runtime.ProcessorSubscription;
import com.linecorp.decaton.processor.runtime.ProcessorsBuilder;
import com.linecorp.decaton.processor.runtime.Property;
import com.linecorp.decaton.processor.runtime.StaticPropertySupplier;
import com.linecorp.decaton.processor.runtime.internal.RateLimiter;
import com.linecorp.decaton.protobuf.ProtocolBuffersDeserializer;
import com.linecorp.decaton.protocol.Sample.HelloTask;
import com.linecorp.decaton.testing.KafkaClusterRule;
import com.linecorp.decaton.testing.TestUtils;
import com.linecorp.decaton.testing.processor.ProcessingGuarantee.GuaranteeType;
import com.linecorp.decaton.testing.processor.ProcessorTestSuite;

public class PerKeyQuotaTest {
    @ClassRule
    public static KafkaClusterRule rule = new KafkaClusterRule();

    private String topic;
    private String shapingTopic;

    @Before
    public void setUp() {
        topic = rule.admin().createRandomTopic(3, 3);
        shapingTopic = topic + "-shaping";
        rule.admin().createTopic(shapingTopic, 3, 3);
    }

    @After
    public void tearDown() {
        rule.admin().deleteTopics(true, topic, shapingTopic);
    }

    @Test(timeout = 30000)
    public void testShaping() throws Exception {
        Set<String> keys = new HashSet<>();
        for (int i = 0; i < 10000; i++) {
            keys.add("key" + i);
        }

        CountDownLatch quotaLatch = new CountDownLatch(1);
        AtomicLong produced = new AtomicLong(0);
        AtomicLong processed = new AtomicLong(0);
        try (ProcessorSubscription subscription = TestUtils.subscription(
                rule.bootstrapServers(),
                builder -> {
                    builder.processorsBuilder(ProcessorsBuilder.consuming(topic, new ProtocolBuffersDeserializer<>(HelloTask.parser()))
                                                               .thenProcess((context, task) -> processed.incrementAndGet()))
                           .addProperties(StaticPropertySupplier.of(
                                   Property.ofStatic(ProcessorProperties.CONFIG_PER_KEY_QUOTA_PROCESSING_RATE, 1L)
                           ))
                           .enablePerKeyQuota(PerKeyQuotaConfig.shape()
                                                               .toBuilder()
                                                               .window(Duration.ofMillis(50L))
                                                               .callback((key, metric) -> {
                                                                   quotaLatch.countDown();
                                                                   return shapingTopic;
                                                               })
                                                               .build())
                           // Override shaping topic's processing rate to unlimited to speedup test execution
                           .overrideShapingRate(shapingTopic,
                                                StaticPropertySupplier.of(Property.ofStatic(PerKeyQuotaConfig.shapingRateProperty(shapingTopic),
                                                                                            RateLimiter.UNLIMITED)));
                });
             DecatonClient<HelloTask> client = TestUtils.client(topic, rule.bootstrapServers())) {
            while (quotaLatch.getCount() > 0) {
                for (String key : keys) {
                    produced.incrementAndGet();
                    client.put(key, HelloTask.getDefaultInstance());
                }
                quotaLatch.await(10L, TimeUnit.MILLISECONDS);
            }

            TestUtils.awaitCondition(
                    "all produced tasks should be processed",
                    () -> produced.get() == processed.get());
        }
    }

    @Test(timeout = 30000)
    public void testShaping_processingGuarantee() throws Exception {
        ProcessorTestSuite
                .builder(rule)
                .configureProcessorsBuilder(builder -> builder.thenProcess((context, task) -> {}))
                .perKeyQuotaConfig(PerKeyQuotaConfig.shape()
                                                    .toBuilder()
                                                    .window(Duration.ofMillis(50L))
                                                    .build())
                .propertySupplier(StaticPropertySupplier.of(
                        Property.ofStatic(ProcessorProperties.CONFIG_PER_KEY_QUOTA_PROCESSING_RATE, 1L)
                ))
                .excludeSemantics(
                        GuaranteeType.PROCESS_ORDERING,
                        GuaranteeType.SERIAL_PROCESSING)
                .build()
                .run();
    }
}
