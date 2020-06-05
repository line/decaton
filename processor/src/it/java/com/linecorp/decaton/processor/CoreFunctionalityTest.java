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

import java.util.EnumSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.ClassRule;
import org.junit.Test;

import com.linecorp.decaton.testing.KafkaClusterRule;
import com.linecorp.decaton.testing.processor.ProcessingGuarantee.GuaranteeType;
import com.linecorp.decaton.testing.processor.ProcessorTestSuite;

public class CoreFunctionalityTest {
    @ClassRule
    public static KafkaClusterRule rule = new KafkaClusterRule();

    @Test(timeout = 30000)
    public void testProcessConcurrent() {
        ProcessorTestSuite
                .builder(rule)
                .configureProcessorsBuilder(builder -> builder.thenProcess((ctx, task) -> {
                    // adding some pseudo random delay to generate out-of-order completion
                    Thread.sleep(Math.abs(task.hashCode()) % 10);
                }))
                .propertySupplier(StaticPropertySupplier.of(
                        Property.ofStatic(ProcessorProperties.CONFIG_PARTITION_CONCURRENCY, 16)
                ))
                .build()
                .run();
    }

    @Test(timeout = 30000)
    public void testAsyncTaskCompletion() {
        ExecutorService executorService = Executors.newFixedThreadPool(16);
        ProcessorTestSuite
                .builder(rule)
                .configureProcessorsBuilder(builder -> builder.thenProcess((ctx, task) -> {
                    DeferredCompletion completion = ctx.deferCompletion();
                    CompletableFuture.runAsync(() -> {
                        try {
                            Thread.sleep(Math.abs(task.hashCode()) % 10);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(e);
                        } finally {
                            completion.complete();
                        }
                    }, executorService);
                }))
                // If task is completed asynchronously there's no guarantee about
                // completion ordering nor serial processing
                .semantics(EnumSet.of(
                        GuaranteeType.AT_LEAST_ONCE_DELIVERY
                ))
                .build()
                .run();
    }
}
