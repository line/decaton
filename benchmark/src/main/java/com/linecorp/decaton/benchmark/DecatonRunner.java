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

package com.linecorp.decaton.benchmark;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import com.linecorp.decaton.processor.runtime.DecatonTask;
import com.linecorp.decaton.processor.runtime.ProcessorProperties;
import com.linecorp.decaton.processor.runtime.ProcessorsBuilder;
import com.linecorp.decaton.processor.runtime.Property;
import com.linecorp.decaton.processor.runtime.StaticPropertySupplier;
import com.linecorp.decaton.processor.runtime.SubscriptionStateListener;
import com.linecorp.decaton.processor.runtime.TaskExtractor;
import com.linecorp.decaton.processor.TaskMetadata;
import com.linecorp.decaton.processor.runtime.ProcessorScope;
import com.linecorp.decaton.processor.runtime.ProcessorSubscription;
import com.linecorp.decaton.processor.runtime.SubscriptionBuilder;

public class DecatonRunner implements Runner {
    private static final Map<String, Function<String, Object>> propertyConstructors =
            new HashMap<String, Function<String, Object>>() {{
                put(ProcessorProperties.CONFIG_MAX_PENDING_RECORDS.name(), Integer::parseInt);
                put(ProcessorProperties.CONFIG_PARTITION_CONCURRENCY.name(), Integer::parseInt);
                put(ProcessorProperties.CONFIG_LOGGING_MDC_ENABLED.name(), Boolean::parseBoolean);
            }};

    private ProcessorSubscription subscription;

    @Override
    public void init(Config config, Recording recording, ResourceTracker resourceTracker)
            throws InterruptedException {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "decaton-benchmark");
        props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "decaton-benchmark");
        // Unless we reset the offset to the earliest, there's a possibility to hit a timing issue causing
        // some records missing from processing.
        // In particular, when a ProcessorSubscription initiates, it starts calling KafkaConsumer#poll, but
        // at that time the offset might not have initialized (reset) to 0. If we start producing tasks before
        // it completes fetching the current offset from the broker (which is zero), the offset counter might
        // increments before it obtains offset information and consequences to reset the offset to a larger
        // value than zero with the default "latest" reset policy.
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        List<Property<?>> properties = new ArrayList<>();
        for (Map.Entry<String, String> entry : config.parameters().entrySet()) {
            String name = entry.getKey();
            Function<String, Object> ctor = propertyConstructors.get(name);
            Object value = ctor.apply(entry.getValue());
            Property<?> prop = ProcessorProperties.propertyForName(name, value);
            properties.add(prop);
        }

        CountDownLatch startLatch = new CountDownLatch(1);

        subscription = SubscriptionBuilder
                .newBuilder("decaton-benchmark")
                .consumerConfig(props)
                .addProperties(StaticPropertySupplier.of(properties))
                .processorsBuilder(
                        ProcessorsBuilder.consuming(config.topic(),
                                                    (TaskExtractor<Task>) bytes -> {
                                                        Task task = config.taskDeserializer()
                                                                          .deserialize(config.topic(), bytes);
                                                        return new DecatonTask<>(
                                                                TaskMetadata.builder().build(), task, bytes);
                                                    })
                                         .thenProcess(
                                                 () -> (ctx, task) -> {
                                                     resourceTracker.track(Thread.currentThread().getId());
                                                     recording.process(task);
                                                 },
                                                 ProcessorScope.THREAD))
                .stateListener(state -> {
                    if (state == SubscriptionStateListener.State.RUNNING) {
                        startLatch.countDown();
                    }
                })
                .build();
        resourceTracker.track(subscription.getId());
        subscription.start();

        startLatch.await();
    }

    @Override
    public void close() throws Exception {
        if (subscription != null) {
            subscription.close();
        }
    }
}
