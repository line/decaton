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

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import com.linecorp.decaton.processor.runtime.ProcessorProperties;
import com.linecorp.decaton.processor.runtime.ProcessorScope;
import com.linecorp.decaton.processor.runtime.Property;
import com.linecorp.decaton.processor.runtime.StaticPropertySupplier;
import com.linecorp.decaton.processor.internal.HashableByteArray;
import com.linecorp.decaton.testing.KafkaClusterRule;
import com.linecorp.decaton.testing.RandomRule;
import com.linecorp.decaton.testing.processor.ProcessedRecord;
import com.linecorp.decaton.testing.processor.ProcessingGuarantee;
import com.linecorp.decaton.testing.processor.ProcessorTestSuite;
import com.linecorp.decaton.testing.processor.ProducedRecord;
import com.linecorp.decaton.testing.processor.TestTask;

public class CoreFunctionalityTest {
    @ClassRule
    public static KafkaClusterRule rule = new KafkaClusterRule();
    @Rule
    public RandomRule randomRule = new RandomRule();

    @Test(timeout = 30000)
    public void testProcessConcurrent() throws Exception {
        Random rand = randomRule.random();
        ProcessorTestSuite
                .builder(rule)
                .configureProcessorsBuilder(builder -> builder.thenProcess((ctx, task) -> {
                    // adding some random delay to simulate realistic usage
                    Thread.sleep(rand.nextInt(10));
                }))
                .propertySupplier(StaticPropertySupplier.of(
                        Property.ofStatic(ProcessorProperties.CONFIG_PARTITION_CONCURRENCY, 16)
                ))
                .build()
                .run();
    }

    @Test(timeout = 30000)
    public void testProcessConcurrent_PartitionScopeProcessor() throws Exception {
        Random rand = randomRule.random();
        ProcessorTestSuite
                .builder(rule)
                .configureProcessorsBuilder(
                        builder -> builder.thenProcess(
                                () -> (ctx, task) -> Thread.sleep(rand.nextInt(10)), ProcessorScope.PARTITION))
                .propertySupplier(StaticPropertySupplier.of(
                        Property.ofStatic(ProcessorProperties.CONFIG_PARTITION_CONCURRENCY, 16)
                ))
                .build()
                .run();
    }

    @Test(timeout = 30000)
    public void testProcessConcurrent_ThreadScopeProcessor() throws Exception {
        Random rand = randomRule.random();
        ProcessorTestSuite
                .builder(rule)
                .configureProcessorsBuilder(
                        builder -> builder.thenProcess(
                                () -> (ctx, task) -> Thread.sleep(rand.nextInt(10)), ProcessorScope.THREAD))
                .propertySupplier(StaticPropertySupplier.of(
                        Property.ofStatic(ProcessorProperties.CONFIG_PARTITION_CONCURRENCY, 16)
                ))
                .build()
                .run();
    }

    @Test(timeout = 30000)
    public void testAsyncTaskCompletion() throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(16);
        Random rand = randomRule.random();
        ProcessorTestSuite
                .builder(rule)
                .configureProcessorsBuilder(builder -> builder.thenProcess((ctx, task) -> {
                    DeferredCompletion completion = ctx.deferCompletion();
                    executorService.execute(() -> {
                        try {
                            Thread.sleep(rand.nextInt(10));
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(e);
                        } finally {
                            completion.complete();
                        }
                    });
                }))
                .build()
                .run();
    }

    /*
     * This test aims to check if we can complete deferred completion even we get
     * completion instance separately from first deferCompletion() call.
     *
     * NOTE: Though it is a valid way to complete deferred completion like this,
     * it's recommended holding only `Completion` instance if possible to avoid unnecessary
     * heap pressure by holding entire ProcessingContext instance.
     */
    @Test(timeout = 30000)
    public void testGetCompletionInstanceLater() throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(16);
        Random rand = randomRule.random();
        ProcessorTestSuite
                .builder(rule)
                .configureProcessorsBuilder(builder -> builder.thenProcess((ctx, task) -> {
                    ctx.deferCompletion();
                    executorService.execute(() -> {
                        try {
                            Thread.sleep(rand.nextInt(10));
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(e);
                        } finally {
                            ctx.deferCompletion().complete();
                        }
                    });
                }))
                .build()
                .run();
    }

    @Test(timeout = 60000)
    public void testSingleThreadProcessing() throws Exception {
        // Note that this processing semantics is not be considered as Decaton specification which users can rely on.
        // Rather, this is just a expected behavior based on current implementation when we set concurrency to 1.
        ProcessingGuarantee noDuplicates = new ProcessingGuarantee() {
            private final Map<HashableByteArray, List<TestTask>> produced = new HashMap<>();
            private final Map<HashableByteArray, List<TestTask>> processed = new HashMap<>();

            @Override
            public synchronized void onProduce(ProducedRecord record) {
                produced.computeIfAbsent(new HashableByteArray(record.key()), key -> new ArrayList<>()).add(record.task());
            }

            @Override
            public synchronized void onProcess(TaskMetadata metadata, ProcessedRecord record) {
                processed.computeIfAbsent(new HashableByteArray(record.key()), key -> new ArrayList<>()).add(record.task());
            }

            @Override
            public void doAssert() {
                // use assertTrue instead of assertEquals not to cause error message explosion
                //noinspection SimplifiableJUnitAssertion
                assertTrue(produced.equals(processed));
            }
        };

        Random rand = randomRule.random();
        ProcessorTestSuite
                .builder(rule)
                .configureProcessorsBuilder(
                        builder -> builder.thenProcess(
                                (ctx, task) -> Thread.sleep(rand.nextInt(10))))
                .propertySupplier(StaticPropertySupplier.of(
                        Property.ofStatic(ProcessorProperties.CONFIG_PARTITION_CONCURRENCY, 1),
                        Property.ofStatic(ProcessorProperties.CONFIG_MAX_PENDING_RECORDS, 100)
                ))
                .customSemantics(noDuplicates)
                .build()
                .run();
    }

    @Test(timeout = 30000)
    public void testAsyncCompletionWithLeakAndTimeout() throws Exception {
        Random rand = randomRule.random();
        ProcessorTestSuite
                .builder(rule)
                .numTasks(1000)
                .propertySupplier(StaticPropertySupplier.of(Property.ofStatic(
                        ProcessorProperties.CONFIG_DEFERRED_COMPLETE_TIMEOUT_MS, 10L)))
                .configureProcessorsBuilder(builder -> builder.thenProcess((ctx, task) -> {
                    if (rand.nextInt() % 4 == 0) {
                        // Leak 25% offsets
                        ctx.deferCompletion();
                    }
                }))
                .build()
                .run();
    }

    @Test(timeout = 30000)
    public void testCooperativeRebalancing() throws Exception {
        Random rand = randomRule.random();
        Properties consumerConfig = new Properties();
        consumerConfig.setProperty(
                ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                CooperativeStickyAssignor.class.getName());
        ProcessorTestSuite
                .builder(rule)
                .consumerConfig(consumerConfig)
                .configureProcessorsBuilder(builder -> builder.thenProcess(
                        (ctx, task) -> Thread.sleep(rand.nextInt(10))))
                .propertySupplier(StaticPropertySupplier.of(
                        Property.ofStatic(ProcessorProperties.CONFIG_PARTITION_CONCURRENCY, 16)
                ))
                .build()
                .run();
    }
}
