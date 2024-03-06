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

package com.linecorp.decaton.processor.runtime.internal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linecorp.decaton.processor.DecatonProcessor;
import com.linecorp.decaton.processor.metrics.Metrics.TaskMetrics;
import com.linecorp.decaton.processor.runtime.DecatonProcessorSupplier;
import com.linecorp.decaton.processor.runtime.TaskExtractor;

public class Processors<T> {
    private static final Logger logger = LoggerFactory.getLogger(Processors.class);

    private final List<DecatonProcessorSupplier<T>> suppliers;
    private final DecatonProcessorSupplier<byte[]> retryProcessorSupplier;
    private final TaskExtractor<T> taskExtractor;
    private final TaskExtractor<T> retryTaskExtractor;

    public Processors(List<DecatonProcessorSupplier<T>> suppliers,
                      DecatonProcessorSupplier<byte[]> retryProcessorSupplier,
                      TaskExtractor<T> taskExtractor,
                      TaskExtractor<T> retryTaskExtractor) {
        this.suppliers = Collections.unmodifiableList(suppliers);
        this.retryProcessorSupplier = retryProcessorSupplier;
        this.taskExtractor = taskExtractor;
        this.retryTaskExtractor = retryTaskExtractor;
    }

    private DecatonProcessor<byte[]> retryProcessor(ThreadScope scope) {
        if (retryProcessorSupplier != null) {
            return retryProcessorSupplier.getProcessor(scope.subscriptionId(),
                                                       scope.topicPartition(),
                                                       scope.threadId());
        }
        return null;
    }

    private TaskExtractor<T> extractorFromTopic(PartitionScope scope) {
        if (scope.isRetryTopic()) {
            return retryTaskExtractor;
        } else {
            return taskExtractor;
        }
    }

    public ProcessPipeline<T> newPipeline(ThreadScope scope,
                                          ExecutionScheduler scheduler,
                                          TaskMetrics metrics) {
        DecatonProcessor<byte[]> retryProcessor = retryProcessor(scope);

        TaskExtractor<T> taskExtractor = extractorFromTopic(scope);
        try {
            List<DecatonProcessor<T>> processors = createProcessors(scope);
            logger.info("Creating partition processor core: {}", scope);
            return new ProcessPipeline<>(scope, processors, retryProcessor, taskExtractor, scheduler, metrics);
        } catch (Exception e) {
            // Catching Exception instead of RuntimeException, since
            // Kotlin-implemented processor-supplier would throw checked exceptions

            // If exception occurred in the middle of instantiating processors, we have to make sure
            // all the previously created processors are destroyed before bubbling up the exception.
            try {
                destroyThreadScope(scope.subscriptionId(),
                                   scope.topicPartition(),
                                   scope.threadId());
            } catch (Exception e1) {
                // Catching Exception instead of RuntimeException, since
                // Kotlin-implemented processor-supplier would throw checked exceptions

                logger.warn("processor supplier threw exception while leaving thread scope", e1);
            }
            throw e;
        }
    }

    private List<DecatonProcessor<T>> createProcessors(ThreadScope scope) {
        List<DecatonProcessor<T>> processors = new ArrayList<>(suppliers.size());
        for (DecatonProcessorSupplier<T> supplier : suppliers) {
            DecatonProcessor<T> processor = supplier.getProcessor(scope.subscriptionId(),
                                                                  scope.topicPartition(),
                                                                  scope.threadId());
            processors.add(processor);
        }
        return processors;
    }

    public void destroySingletonScope(String subscriptionId) {
        suppliers.forEach(supplier -> supplier.leaveSingletonScope(subscriptionId));
        if (retryProcessorSupplier != null) {
            retryProcessorSupplier.leaveSingletonScope(subscriptionId);
        }
    }

    public void destroyPartitionScope(String subscriptionId, TopicPartition tp) {
        suppliers.forEach(supplier -> supplier.leavePartitionScope(subscriptionId, tp));
        if (retryProcessorSupplier != null) {
            retryProcessorSupplier.leavePartitionScope(subscriptionId, tp);
        }
    }

    public void destroyThreadScope(String subscriptionId, TopicPartition tp, int threadId) {
        suppliers.forEach(supplier -> supplier.leaveThreadScope(subscriptionId, tp, threadId));
        if (retryProcessorSupplier != null) {
            retryProcessorSupplier.leaveThreadScope(subscriptionId, tp, threadId);
        }
    }
}
