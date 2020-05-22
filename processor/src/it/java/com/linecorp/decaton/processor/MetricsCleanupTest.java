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

import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import com.linecorp.decaton.client.DecatonClient;
import com.linecorp.decaton.processor.metrics.Metrics;
import com.linecorp.decaton.processor.runtime.ProcessorSubscription;
import com.linecorp.decaton.protobuf.ProtocolBuffersDeserializer;
import com.linecorp.decaton.protocol.Sample.HelloTask;
import com.linecorp.decaton.testing.KafkaClusterRule;
import com.linecorp.decaton.testing.TestUtils;

import io.micrometer.core.instrument.Meter;

public class MetricsCleanupTest {
    @ClassRule
    public static KafkaClusterRule rule = new KafkaClusterRule();

    private String topicName;

    @Before
    public void setUp() {
        topicName = rule.admin().createRandomTopic(3, 3);
    }

    @After
    public void tearDown() {
        rule.admin().deleteTopics(topicName);
    }

    @Test(timeout = 30000)
    public void testMetricsCleanup() throws Exception {
        CountDownLatch processLatch = new CountDownLatch(1);
        try (ProcessorSubscription subscription = TestUtils.subscription(
                rule.bootstrapServers(),
                ProcessorsBuilder.consuming(topicName, new ProtocolBuffersDeserializer<>(HelloTask.parser()))
                                 .thenProcess((context, task) -> processLatch.countDown()),
                null,
                StaticPropertySupplier.of());
             DecatonClient<HelloTask> client = TestUtils.client(topicName, rule.bootstrapServers())) {
            client.put(null, HelloTask.getDefaultInstance());
            processLatch.await();
        }

        List<Meter> meters = Metrics.registry().getMeters();
        assertEquals(emptyList(), meters);
    }
}
