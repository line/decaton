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

package com.linecorp.decaton.processor.runtime;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

import java.util.Properties;

public class SubscriptionBuilderTest {

    @Test
    public void testProducerConfigSupplier() {
        Properties consumerConfig = new Properties();
        consumerConfig.setProperty("bootstrap.servers", "localhost:9092");
        consumerConfig.setProperty("max.poll.records", "10");
        consumerConfig.setProperty("group.id", "group-a");
        consumerConfig.setProperty("metric.reporters", "org.apache.kafka.common.metrics.MetricsReporter");
        Properties producerConfig = SubscriptionBuilder.producerConfigSupplier(consumerConfig).get();
        assertEquals(2, producerConfig.size());
        assertEquals("localhost:9092", producerConfig.getProperty("bootstrap.servers"));
        assertEquals("org.apache.kafka.common.metrics.MetricsReporter",
                     producerConfig.getProperty("metric.reporters"));
    }
}
