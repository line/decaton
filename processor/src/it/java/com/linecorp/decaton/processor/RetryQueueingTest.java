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
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import com.linecorp.decaton.processor.runtime.RetryConfig;
import com.linecorp.decaton.testing.KafkaClusterRule;
import com.linecorp.decaton.testing.TestUtils;
import com.linecorp.decaton.testing.processor.ProcessedRecord;
import com.linecorp.decaton.testing.processor.ProcessingGuarantee;
import com.linecorp.decaton.testing.processor.ProcessingGuarantee.GuaranteeType;
import com.linecorp.decaton.testing.processor.ProcessorTestSuite;
import com.linecorp.decaton.testing.processor.ProducedRecord;

public class RetryQueueingTest {
    @ClassRule
    public static KafkaClusterRule rule = new KafkaClusterRule();

    private String retryTopic;

    @Before
    public void setUp() {
        retryTopic = rule.admin().createRandomTopic(3, 3);
    }

    @After
    public void tearDown() {
        rule.admin().deleteTopics(retryTopic);
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

    @Test(timeout = 30000)
    public void testRetryQueuing() throws Exception {
        // scenario:
        //   * all arrived tasks are retried once
        //   * after retried (i.e. retryCount() > 0), no more retry
        ProcessorTestSuite
                .builder(rule)
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
}
