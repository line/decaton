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

import java.time.Duration;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

import com.linecorp.decaton.processor.Completion.TimeoutChoice;
import com.linecorp.decaton.processor.runtime.ConsumedRecord;
import com.linecorp.decaton.processor.runtime.DecatonTask;
import com.linecorp.decaton.processor.runtime.DynamicProperty;
import com.linecorp.decaton.processor.runtime.ProcessorProperties;
import com.linecorp.decaton.processor.runtime.Property;
import com.linecorp.decaton.processor.runtime.RetryConfig;
import com.linecorp.decaton.processor.runtime.StaticPropertySupplier;
import com.linecorp.decaton.processor.runtime.TaskExtractor;
import com.linecorp.decaton.testing.KafkaClusterExtension;
import com.linecorp.decaton.testing.TestUtils;
import com.linecorp.decaton.testing.processor.ProcessedRecord;
import com.linecorp.decaton.testing.processor.ProcessingGuarantee;
import com.linecorp.decaton.testing.processor.ProcessingGuarantee.GuaranteeType;
import com.linecorp.decaton.testing.processor.ProcessorTestSuite;
import com.linecorp.decaton.testing.processor.ProducedRecord;
import com.linecorp.decaton.testing.processor.TestTask;

public class RetryQueueingTest {
    @RegisterExtension
    public static KafkaClusterExtension rule = new KafkaClusterExtension(brokerProperties());

    private String retryTopic;

    // We found that retry record's timestamp could be set to 0 unintentionally,
    // which causes messages to be deleted by retention immediately.
    // To reproduce it in integration test easily, we set shorter retention check interval.
    // See https://github.com/line/decaton/issues/101 for the details.
    private static final long RETENTION_CHECK_INTERVAL_MS = 500L;

    private static Properties brokerProperties() {
        Properties props = new Properties();
        props.setProperty("log.retention.check.interval.ms", String.valueOf(RETENTION_CHECK_INTERVAL_MS));
        return props;
    }

    @BeforeEach
    public void setUp() {
        retryTopic = rule.admin().createRandomTopic(3, 3);
    }

    @AfterEach
    public void tearDown() {
        rule.admin().deleteTopics(true, retryTopic);
    }

    private static class ProcessRetriedTask implements ProcessingGuarantee {
        private final Set<String> producedIds = ConcurrentHashMap.newKeySet();
        private final Set<String> processedIds = ConcurrentHashMap.newKeySet();

        @Override
        public void onProduce(ProducedRecord record) {
            producedIds.add(record.task().getId());
        }

        @Override
        public void onProcess(TaskMetadata metadata, ProcessedRecord record) {
            if (metadata.retryCount() > 0) {
                processedIds.add(record.task().getId());
            }
        }

        @Override
        public void doAssert() {
            try {
                TestUtils.awaitCondition("all retried tasks must be processed",
                                         () -> producedIds.size() == processedIds.size());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }

    private static class TestTaskExtractor implements TaskExtractor<TestTask> {
        @Override
        public DecatonTask<TestTask> extract(ConsumedRecord record) {
            TaskMetadata meta = TaskMetadata.builder().build();
            TestTask task = new TestTask.TestTaskDeserializer().deserialize(record.value());
            return new DecatonTask<>(meta, task, record.value());
        }
    }

    @Test
    @Timeout(30)
    public void testRetryQueuing() throws Exception {
        // scenario:
        //   * all arrived tasks are retried once
        //   * after retried (i.e. retryCount() > 0), no more retry
        ProcessorTestSuite
                .builder(rule)
                .numTasks(1000)
                .configureProcessorsBuilder(builder -> builder.thenProcess((ctx, task) -> {
                    if (ctx.metadata().retryCount() == 0) {
                        ctx.retry();
                    }
                }))
                .retryConfig(RetryConfig.builder()
                                        .retryTopic(retryTopic)
                                        .backoff(Duration.ofMillis(10))
                                        .build())
                // If we retry tasks, there's no guarantee about ordering nor serial processing
                .excludeSemantics(
                        GuaranteeType.PROCESS_ORDERING,
                        GuaranteeType.SERIAL_PROCESSING)
                .customSemantics(new ProcessRetriedTask())
                .build()
                .run();
    }

    @Test
    @Timeout(30)
    public void testRetryQueuingOnAsyncProcessor() throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(16);
        ProcessorTestSuite
                .builder(rule)
                .numTasks(1000)
                .configureProcessorsBuilder(builder -> builder.thenProcess((ctx, task) -> {
                    ctx.deferCompletion();
                    executorService.execute(() -> {
                        try {
                            if (ctx.metadata().retryCount() == 0) {
                                ctx.retry();
                            } else {
                                ctx.deferCompletion().complete();
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(e);
                        }
                    });

                }))
                .retryConfig(RetryConfig.builder()
                                        .retryTopic(retryTopic)
                                        .backoff(Duration.ofMillis(10))
                                        .build())
                .excludeSemantics(
                        GuaranteeType.PROCESS_ORDERING,
                        GuaranteeType.SERIAL_PROCESSING)
                .customSemantics(new ProcessRetriedTask())
                .build()
                .run();
    }

    /*
     * This test tries to re-produce delivery-loss due to https://github.com/line/decaton/issues/101 by:
     *   1. Retry all tasks once immediately
     *   2. Process retried tasks in "slow" pace
     *     - To simulate the situation like "Retry tasks spikes (e.g. due to downstream storage failure),
     *       but processing throughput is not sufficient", which is typical situation that causes
     *       message delivery loss due to decaton#101.
     */
    @Test
    @Timeout(60)
    public void testRetryQueuingExtractingWithDefaultMeta() throws Exception {
        // LogManager waits to start clean-up process until InitialTaskDelayMs (30sec) elapses.
        // https://github.com/apache/kafka/blob/2.4.0/core/src/main/scala/kafka/log/LogManager.scala#L70
        Thread.sleep(30000L);

        AtomicBoolean firstRetryTask = new AtomicBoolean(false);
        ProcessorTestSuite
                .builder(rule)
                .numTasks(10)
                .propertySupplier(StaticPropertySupplier.of(
                        // Configure max pending records to 1 to pause fetches easily
                        Property.ofStatic(ProcessorProperties.CONFIG_MAX_PENDING_RECORDS, 1),
                        Property.ofStatic(ProcessorProperties.CONFIG_PARTITION_CONCURRENCY, 1)
                ))
                .configureProcessorsBuilder(builder -> builder.thenProcess((ctx, task) -> {
                    if (ctx.metadata().retryCount() == 0) {
                        ctx.retry();
                    } else if (firstRetryTask.compareAndSet(false, true)) {
                        // Waiting certain time for first retry task so that
                        // subsequent retry tasks are processed after log-retention-check executed.
                        Thread.sleep(RETENTION_CHECK_INTERVAL_MS * 2);
                    }
                }))
                .retryConfig(RetryConfig.builder()
                                        .retryTopic(retryTopic)
                                        .backoff(Duration.ofMillis(10))
                                        .build())
                .excludeSemantics(
                        GuaranteeType.PROCESS_ORDERING,
                        GuaranteeType.SERIAL_PROCESSING)
                .customSemantics(new ProcessRetriedTask())
                .customTaskExtractor(new TestTaskExtractor())
                .build()
                .run();
    }

    @Test
    @Timeout(60)
    public void testRetryQueueingFromCompletionTimeoutCallback() throws Exception {
        ProcessorTestSuite
                .builder(rule)
                .numTasks(100)
                .propertySupplier(StaticPropertySupplier.of(Property.ofStatic(
                        ProcessorProperties.CONFIG_DEFERRED_COMPLETE_TIMEOUT_MS, 100L)))
                .configureProcessorsBuilder(builder -> builder.thenProcess((ctx, task) -> {
                    if (ctx.metadata().retryCount() == 0) {
                        // Leak completion
                        ctx.deferCompletion(comp -> {
                            try {
                                comp.completeWith(ctx.retry());
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                            return TimeoutChoice.EXTEND;
                        });
                    }
                }))
                .retryConfig(RetryConfig.builder()
                                        .retryTopic(retryTopic)
                                        .backoff(Duration.ofMillis(10))
                                        .build())
                .excludeSemantics(GuaranteeType.PROCESS_ORDERING,
                                  GuaranteeType.SERIAL_PROCESSING)
                .customSemantics(new ProcessRetriedTask())
                .build()
                .run();
    }

    @Test
    @Timeout(60)
    public void testRetryQueueingMigrateToHeader() throws Exception {
        DynamicProperty<Boolean> retryTaskInLegacyFormat =
                new DynamicProperty<>(ProcessorProperties.CONFIG_RETRY_TASK_IN_LEGACY_FORMAT);
        retryTaskInLegacyFormat.set(true);

        AtomicInteger processCount = new AtomicInteger(0);
        CountDownLatch migrationLatch = new CountDownLatch(1);
        ProcessorTestSuite
                .builder(rule)
                .numTasks(100)
                .propertySupplier(StaticPropertySupplier.of(
                        retryTaskInLegacyFormat,
                        Property.ofStatic(ProcessorProperties.CONFIG_LEGACY_PARSE_FALLBACK_ENABLED, true)))
                .produceTasksWithHeaderMetadata(false)
                .configureProcessorsBuilder(builder -> builder.thenProcess((ctx, task) -> {
                    if (ctx.metadata().retryCount() == 0) {
                        int cnt = processCount.incrementAndGet();
                        // Enable header-mode after 50 tasks are processed
                        if (cnt < 50) {
                            ctx.retry();
                        } else if (cnt == 50) {
                            retryTaskInLegacyFormat.set(true);
                            migrationLatch.countDown();
                            ctx.retry();
                        } else {
                            migrationLatch.await();
                            ctx.retry();
                        }
                    }
                }))
                .retryConfig(RetryConfig.builder()
                                        .retryTopic(retryTopic)
                                        .backoff(Duration.ofMillis(10))
                                        .build())
                .excludeSemantics(GuaranteeType.PROCESS_ORDERING,
                                  GuaranteeType.SERIAL_PROCESSING)
                .customSemantics(new ProcessRetriedTask())
                .build()
                .run();
    }
}
