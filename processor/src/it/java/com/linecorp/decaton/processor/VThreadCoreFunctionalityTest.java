/*
 * Copyright 2024 LY Corporation
 *
 * LY Corporation licenses this file to you under the Apache License,
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

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

import com.linecorp.decaton.processor.runtime.ProcessorProperties;
import com.linecorp.decaton.processor.runtime.ProcessorScope;
import com.linecorp.decaton.processor.runtime.Property;
import com.linecorp.decaton.processor.runtime.StaticPropertySupplier;
import com.linecorp.decaton.processor.runtime.SubPartitionRuntime;
import com.linecorp.decaton.testing.KafkaClusterExtension;
import com.linecorp.decaton.testing.RandomExtension;
import com.linecorp.decaton.testing.processor.ProcessorTestSuite;

public class VThreadCoreFunctionalityTest {
    @RegisterExtension
    public static KafkaClusterExtension rule = new KafkaClusterExtension();
    @RegisterExtension
    public RandomExtension randomExtension = new RandomExtension();

    @Test
    @Timeout(30)
    public void testProcessConcurrent() throws Exception {
        Random rand = randomExtension.random();
        ProcessorTestSuite
                .builder(rule)
                .subPartitionRuntime(SubPartitionRuntime.VIRTUAL_THREAD)
                .configureProcessorsBuilder(builder -> builder.thenProcess((ctx, task) -> {
                    // adding some random delay to simulate realistic usage
                    Thread.sleep(rand.nextInt(10));
                }))
                .build()
                .run();
    }

    @Test
    @Timeout(30)
    public void testProcessConcurrent_PartitionScopeProcessor() throws Exception {
        Random rand = randomExtension.random();
        ProcessorTestSuite
                .builder(rule)
                .subPartitionRuntime(SubPartitionRuntime.VIRTUAL_THREAD)
                .configureProcessorsBuilder(
                        builder -> builder.thenProcess(
                                () -> (ctx, task) -> Thread.sleep(rand.nextInt(10)), ProcessorScope.PARTITION))
                .build()
                .run();
    }

    @Test
    @Timeout(30)
    public void testProcessConcurrent_ThreadScopeProcessor() throws Exception {
        Random rand = randomExtension.random();
        ProcessorTestSuite
                .builder(rule)
                .subPartitionRuntime(SubPartitionRuntime.VIRTUAL_THREAD)
                .configureProcessorsBuilder(
                        builder -> builder.thenProcess(
                                () -> (ctx, task) -> Thread.sleep(rand.nextInt(10)), ProcessorScope.THREAD))
                .build()
                .run();
    }

    @Test
    @Timeout(30)
    public void testAsyncTaskCompletion() throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(16);
        Random rand = randomExtension.random();
        ProcessorTestSuite
                .builder(rule)
                .subPartitionRuntime(SubPartitionRuntime.VIRTUAL_THREAD)
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
    @Test
    @Timeout(30)
    public void testGetCompletionInstanceLater() throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(16);
        Random rand = randomExtension.random();
        ProcessorTestSuite
                .builder(rule)
                .subPartitionRuntime(SubPartitionRuntime.VIRTUAL_THREAD)
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

    @Test
    @Timeout(30)
    public void testAsyncCompletionWithLeakAndTimeout() throws Exception {
        Random rand = randomExtension.random();
        ProcessorTestSuite
                .builder(rule)
                .numTasks(1000)
                .subPartitionRuntime(SubPartitionRuntime.VIRTUAL_THREAD)
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
}
