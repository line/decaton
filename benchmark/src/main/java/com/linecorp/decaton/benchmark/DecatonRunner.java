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

import java.lang.management.ThreadInfo;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import com.sun.management.ThreadMXBean;

import com.linecorp.decaton.processor.TaskMetadata;
import com.linecorp.decaton.processor.metrics.Metrics;
import com.linecorp.decaton.processor.runtime.DecatonTask;
import com.linecorp.decaton.processor.runtime.ProcessorProperties;
import com.linecorp.decaton.processor.runtime.ProcessorSubscription;
import com.linecorp.decaton.processor.runtime.ProcessorsBuilder;
import com.linecorp.decaton.processor.runtime.Property;
import com.linecorp.decaton.processor.runtime.StaticPropertySupplier;
import com.linecorp.decaton.processor.runtime.SubPartitionRuntime;
import com.linecorp.decaton.processor.runtime.SubscriptionBuilder;
import com.linecorp.decaton.processor.runtime.SubscriptionStateListener;
import com.linecorp.decaton.processor.runtime.TaskExtractor;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.logging.LoggingMeterRegistry;
import io.micrometer.core.instrument.logging.LoggingRegistryConfig;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DecatonRunner implements Runner {
    private static final Map<String, Function<String, Object>> propertyConstructors =
            new HashMap<String, Function<String, Object>>() {{
                put(ProcessorProperties.CONFIG_MAX_PENDING_RECORDS.name(), Integer::parseInt);
                put(ProcessorProperties.CONFIG_PARTITION_CONCURRENCY.name(), Integer::parseInt);
                put(ProcessorProperties.CONFIG_LOGGING_MDC_ENABLED.name(), Boolean::parseBoolean);
            }};

    private SubPartitionRuntime subPartitionRuntime;
    private ProcessorSubscription subscription;
    private LoggingMeterRegistry registry;

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

        subPartitionRuntime = SubPartitionRuntime.THREAD_POOL;
        List<Property<?>> properties = new ArrayList<>();
        for (Map.Entry<String, String> entry : config.parameters().entrySet()) {
            String name = entry.getKey();
            if ("decaton.subpartition.runtime".equals(name)) {
                subPartitionRuntime = SubPartitionRuntime.valueOf(entry.getValue());
            } else {
                Function<String, Object> ctor = propertyConstructors.get(name);
                Object value = ctor.apply(entry.getValue());
                Property<?> prop = ProcessorProperties.propertyForName(name, value);
                properties.add(prop);
            }
        }

        registry = new LoggingMeterRegistry(new LoggingRegistryConfig() {
            @Override
            public Duration step() {
                return Duration.ofSeconds(10);
            }
            @Override
            public String get(String key) {
                return null;
            }
        }, Clock.SYSTEM);
        Metrics.register(registry);

        maybeSetupForkJoinPoolDrip();

        CountDownLatch startLatch = new CountDownLatch(1);

        subscription = SubscriptionBuilder
                .newBuilder("decaton-benchmark")
                .consumerConfig(props)
                .addProperties(StaticPropertySupplier.of(properties))
                .subPartitionRuntime(subPartitionRuntime)
                .processorsBuilder(
                        ProcessorsBuilder.consuming(config.topic(),
                                                    (TaskExtractor<Task>) bytes -> {
                                                        Task task = config.taskDeserializer()
                                                                          .deserialize(config.topic(), bytes);
                                                        return new DecatonTask<>(
                                                                TaskMetadata.builder().build(), task, bytes);
                                                    })
                                         .thenProcess(
                                                 (ctx, task) -> recording.process(task)))
                .stateListener(state -> {
                    if (state == SubscriptionStateListener.State.RUNNING) {
                        startLatch.countDown();
                    }
                })
                .build();
        subscription.start();

        startLatch.await();
    }

    /**
     * This value comes from VirtualThread#createDefaultScheduler(), which defaults keep-alive of ForkJoinPool
     * to 30 seconds (as of Java21).
     */
    private static final long DRIP_INTERVAL = 30;
    /**
     * The intention of this method is to setup a scheduled work submitting some VirtualThread instances
     * periodically, in order to make sure the {@link ForkJoinPool} to keep-alive the created carrier thread
     * during warmup phase.
     * It is necessary because {@link #onWarmupComplete(ResourceTracker)} depends on the alive list of
     * ForkJoinPool threads to observe cpu time and memory allocation profile, which cannot be obtained from
     * VirtualThread instances directly.
     */
    private void maybeSetupForkJoinPoolDrip() {
        if (subPartitionRuntime != SubPartitionRuntime.VIRTUAL_THREAD) {
            return;
        }
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(
                1, Thread.ofPlatform().daemon().factory());
        executor.scheduleAtFixedRate(() -> {
            for (int i = 0; i < Runtime.getRuntime().availableProcessors(); i++) {
                Thread.startVirtualThread(() -> {
                    try {
                        TimeUnit.SECONDS.sleep(DRIP_INTERVAL);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });
            }
        }, 0, DRIP_INTERVAL, TimeUnit.SECONDS);
    }

    @Override
    public void onWarmupComplete(ResourceTracker resourceTracker) {
        // As we support VIRTUAL_THREAD runtime now, we can't use the technique to obtain the target Thread ID
        // in the execution context, as in VirtualThread gets created and discarded for all tasks separately.
        // Instead we need to observe resource usage based on the carrier thread (platform thread) side, while
        // stdlib doesn't provide a way to resolve the carrier thread's ID.
        ThreadMXBean threadMxBean = ResourceTracker.getSunThreadMxBean();
        ThreadInfo[] threadInfos = threadMxBean.dumpAllThreads(true, true);
        for (ThreadInfo threadInfo : threadInfos) {
            String name = threadInfo.getThreadName();
            log.debug("Tracking target check for thread name: {}", name);
            if (name.startsWith("DecatonSubscriptionThread-") || name.startsWith("PartitionProcessorThread-") ||
                subPartitionRuntime == SubPartitionRuntime.VIRTUAL_THREAD && name.startsWith("ForkJoinPool-")) {
                log.info("Tracking resource of thread {} - {}", threadInfo.getThreadId(), name);
                resourceTracker.track(threadInfo.getThreadId());
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (registry != null) {
            registry.close();
        }
        if (subscription != null) {
            subscription.close();
        }
    }
}
