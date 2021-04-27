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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import com.linecorp.decaton.processor.runtime.DecatonTask;
import com.linecorp.decaton.processor.runtime.ProcessorProperties;
import com.linecorp.decaton.processor.runtime.Property;
import com.linecorp.decaton.processor.runtime.RetryConfig;
import com.linecorp.decaton.processor.runtime.StaticPropertySupplier;
import com.linecorp.decaton.processor.runtime.TaskExtractor;
import com.linecorp.decaton.protocol.Decaton.DecatonTaskRequest;
import com.linecorp.decaton.testing.KafkaClusterRule;
import com.linecorp.decaton.testing.TestUtils;
import com.linecorp.decaton.testing.processor.ProcessedRecord;
import com.linecorp.decaton.testing.processor.ProcessingGuarantee;
import com.linecorp.decaton.testing.processor.ProcessingGuarantee.GuaranteeType;
import com.linecorp.decaton.testing.processor.ProcessorTestSuite;
import com.linecorp.decaton.testing.processor.ProducedRecord;
import com.linecorp.decaton.testing.processor.TestTask;

public class RetryQueueingTest {
    @ClassRule
    public static KafkaClusterRule rule = new KafkaClusterRule(brokerProperties());

    private String retryTopic;

    private static Properties brokerProperties() {
        Properties props = new Properties();
        // We found that retry record's timestamp could be set to 0 unintentionally,
        // which causes messages to be deleted by retention immediately.
        // To reproduce it in integration test easily, we set shorter retention check interval.
        // See https://github.com/line/decaton/issues/101 for the details.
        props.setProperty("log.retention.check.interval.ms", "500");
        return props;
    }

    @Before
    public void setUp() {
        retryTopic = rule.admin().createRandomTopic(3, 3);
    }

    @After
    public void tearDown() {
        rule.admin().deleteTopics(true, retryTopic);
    }

    private static class ProcessRetriedTask implements ProcessingGuarantee {
        private final Set<String> producedIds = Collections.synchronizedSet(new HashSet<>());
        private final Set<String> processedIds = Collections.synchronizedSet(new HashSet<>());

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
            TestUtils.awaitCondition("all retried tasks must be processed",
                                     () -> producedIds.size() == processedIds.size());
        }
    }

    private static class TestTaskExtractor implements TaskExtractor<TestTask> {
        @Override
        public DecatonTask<TestTask> extract(byte[] bytes) {
            try {
                TaskMetadata meta = TaskMetadata.builder().build();
                DecatonTaskRequest request = DecatonTaskRequest.parseFrom(bytes);
                TestTask task = new TestTask.TestTaskDeserializer().deserialize(
                        request.getSerializedTask().toByteArray());
                return new DecatonTask<>(meta, task, bytes);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    @Test(timeout = 30000)
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

    /*
     * This test tries to re-produce delivery-loss due to https://github.com/line/decaton/issues/101 by:
     *   1. Retry all tasks once immediately
     *   2. Process retried tasks in "slow" pace
     *     - To simulate the situation like "Retry tasks spikes (e.g. due to downstream storage failure),
     *       but processing throughput is not sufficient", which is typical situation that causes
     *       message delivery loss due to decaton#101.
     */
    @Test(timeout = 60000)
    public void testRetryQueuingExtractingWithDefaultMeta() throws Exception {
        // LogManager waits to start clean-up process until InitialTaskDelayMs (30sec) elapses.
        // https://github.com/apache/kafka/blob/2.4.0/core/src/main/scala/kafka/log/LogManager.scala#L70
        Thread.sleep(30000L);

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
                    } else {
                        Thread.sleep(1000L);
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
}
