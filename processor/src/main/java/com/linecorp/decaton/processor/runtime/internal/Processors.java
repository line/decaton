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

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linecorp.decaton.processor.Completion;
import com.linecorp.decaton.processor.Completion.TimeoutChoice;
import com.linecorp.decaton.processor.DecatonProcessor;
import com.linecorp.decaton.processor.LoggingContext;
import com.linecorp.decaton.processor.ProcessingContext;
import com.linecorp.decaton.processor.TaskMetadata;
import com.linecorp.decaton.processor.runtime.TaskExtractor;
import com.linecorp.decaton.processor.metrics.Metrics;
import com.linecorp.decaton.processor.runtime.DecatonProcessorSupplier;

public class Processors<T> {
    private static final Logger logger = LoggerFactory.getLogger(Processors.class);

    private final List<DecatonProcessorSupplier<T>> suppliers;
    private final DecatonProcessorSupplier<byte[]> retryProcessorSupplier;
    private final DecatonProcessorSupplier<QuotaAwareTask<T>> shapingProcessorSupplier;
    private final TaskExtractor<T> taskExtractor;
    private final TaskExtractor<T> decatonQueuedTaskExtractor;

    public Processors(List<DecatonProcessorSupplier<T>> suppliers,
                      DecatonProcessorSupplier<byte[]> retryProcessorSupplier,
                      DecatonProcessorSupplier<QuotaAwareTask<T>> shapingProcessorSupplier,
                      TaskExtractor<T> taskExtractor,
                      TaskExtractor<T> decatonQueuedTaskExtractor) {
        this.suppliers = Collections.unmodifiableList(suppliers);
        this.retryProcessorSupplier = retryProcessorSupplier;
        this.shapingProcessorSupplier = shapingProcessorSupplier;
        this.taskExtractor = taskExtractor;
        this.decatonQueuedTaskExtractor = decatonQueuedTaskExtractor;
    }

    private DecatonProcessor<byte[]> retryProcessor(ThreadScope scope) {
        if (retryProcessorSupplier != null) {
            return retryProcessorSupplier.getProcessor(scope.subscriptionId(),
                                                       scope.topicPartition(),
                                                       scope.threadId());
        }
        return null;
    }

    private DecatonProcessor<QuotaAwareTask<T>> shapingProcessor(ThreadScope scope) {
        if (shapingProcessorSupplier != null && !scope.isShapingTopic()) {
            return shapingProcessorSupplier.getProcessor(scope.subscriptionId(),
                                                         scope.topicPartition(),
                                                         scope.threadId());
        }
        return null;
    }

    private TaskExtractor<T> extractorFromTopic(PartitionScope scope) {
        if (scope.isRetryTopic() || scope.isShapingTopic()) {
            return decatonQueuedTaskExtractor;
        } else {
            return taskExtractor;
        }
    }

    public ProcessPipeline<T> newPipeline(ThreadScope scope,
                                          ExecutionScheduler scheduler,
                                          Metrics metrics) {
        DecatonProcessor<byte[]> retryProcessor = retryProcessor(scope);
        DecatonProcessor<QuotaAwareTask<T>> shapingProcessor = shapingProcessor(scope);

        TaskExtractor<T> taskExtractor = extractorFromTopic(scope);
        try {
            List<DecatonProcessor<QuotaAwareTask<T>>> processors =
                    Stream.concat(
                            Optional.ofNullable(shapingProcessor)
                                    .map(Stream::of)
                                    .orElse(Stream.empty()),
                            suppliers.stream()
                                     .map(s -> s.getProcessor(scope.subscriptionId(),
                                                              scope.topicPartition(),
                                                              scope.threadId()))
                                     .map(Processors::asQuotaAware)
                    ).collect(Collectors.toList());
            logger.info("Creating partition processor core: {}", scope);
            return new ProcessPipeline<>(scope, processors, retryProcessor, taskExtractor, scheduler, metrics);
        } catch (RuntimeException e) {
            // If exception occurred in the middle of instantiating processors, we have to make sure
            // all the previously created processors are destroyed before bubbling up the exception.
            try {
                destroyThreadScope(scope.subscriptionId(),
                                   scope.topicPartition(),
                                   scope.threadId());
            } catch (RuntimeException e1) {
                logger.warn("processor supplier threw exception while leaving thread scope", e1);
            }
            throw e;
        }
    }

    public void destroySingletonScope(String subscriptionId) {
        suppliers.forEach(supplier -> supplier.leaveSingletonScope(subscriptionId));
        if (retryProcessorSupplier != null) {
            retryProcessorSupplier.leaveSingletonScope(subscriptionId);
        }
        if (shapingProcessorSupplier != null) {
            shapingProcessorSupplier.leaveSingletonScope(subscriptionId);
        }
    }

    public void destroyPartitionScope(String subscriptionId, TopicPartition tp) {
        suppliers.forEach(supplier -> supplier.leavePartitionScope(subscriptionId, tp));
        if (retryProcessorSupplier != null) {
            retryProcessorSupplier.leavePartitionScope(subscriptionId, tp);
        }
        if (shapingProcessorSupplier != null) {
            shapingProcessorSupplier.leavePartitionScope(subscriptionId, tp);
        }
    }

    public void destroyThreadScope(String subscriptionId, TopicPartition tp, int threadId) {
        suppliers.forEach(supplier -> supplier.leaveThreadScope(subscriptionId, tp, threadId));
        if (retryProcessorSupplier != null) {
            retryProcessorSupplier.leaveThreadScope(subscriptionId, tp, threadId);
        }
        if (shapingProcessorSupplier != null) {
            shapingProcessorSupplier.leaveThreadScope(subscriptionId, tp, threadId);
        }
    }

    private static <T> DecatonProcessor<QuotaAwareTask<T>> asQuotaAware(DecatonProcessor<T> processor) {
        return (ctx, task) -> {
            ProcessingContext<T> wrapped = new ProcessingContext<T>() {
                @Override
                public TaskMetadata metadata() {
                    return ctx.metadata();
                }

                @Override
                public byte[] key() {
                    return ctx.key();
                }

                @Override
                public Headers headers() {
                    return ctx.headers();
                }

                @Override
                public String subscriptionId() {
                    return ctx.subscriptionId();
                }

                @Override
                public LoggingContext loggingContext() {
                    return ctx.loggingContext();
                }

                @Override
                public Completion deferCompletion(Function<Completion, TimeoutChoice> callback) {
                    return ctx.deferCompletion(callback);
                }

                @Override
                public Completion push(T ignore) throws InterruptedException {
                    return ctx.push(task);
                }

                @Override
                public Completion retry() throws InterruptedException {
                    return ctx.retry();
                }
            };
            processor.process(wrapped, task.task());
        };
    }
}
