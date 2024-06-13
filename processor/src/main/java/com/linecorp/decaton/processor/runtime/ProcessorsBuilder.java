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

package com.linecorp.decaton.processor.runtime;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.linecorp.decaton.common.Deserializer;
import com.linecorp.decaton.processor.DecatonProcessor;
import com.linecorp.decaton.processor.runtime.internal.DecatonProcessorSupplierImpl;
import com.linecorp.decaton.processor.runtime.internal.DefaultTaskExtractor;
import com.linecorp.decaton.processor.runtime.internal.Processors;

import lombok.Getter;
import lombok.experimental.Accessors;

/**
 * A class defines processing pipeline for {@link ProcessorSubscription}.
 *
 * @param <T> type of tasks to be processed.
 */
@Accessors(fluent = true)
public class ProcessorsBuilder<T> {
    @Getter
    private final String topic;
    private final TaskExtractor<T> taskExtractor;
    private final TaskExtractor<T> retryTaskExtractor;

    private final List<DecatonProcessorSupplier<T>> suppliers;

    public ProcessorsBuilder(String topic, TaskExtractor<T> taskExtractor, TaskExtractor<T> retryTaskExtractor) {
        this.topic = topic;
        this.taskExtractor = taskExtractor;
        this.retryTaskExtractor = retryTaskExtractor;
        suppliers = new ArrayList<>();
    }

    /**
     * Create new {@link ProcessorsBuilder} that consumes message from topic expecting tasks of type
     * which can be parsed by valueParser.
     * @param topic the name of topic to consume.
     * @param deserializer the deserializer to instantiate task of type {@link T} from serialized bytes.
     * @param <T> the type of instantiated tasks.
     * @return an instance of {@link ProcessorsBuilder}.
     */
    public static <T> ProcessorsBuilder<T> consuming(String topic, Deserializer<T> deserializer) {
        DefaultTaskExtractor<T> taskExtractor = new DefaultTaskExtractor<>(deserializer);
        return new ProcessorsBuilder<>(topic, taskExtractor, taskExtractor);
    }

    /**
     * Create new {@link ProcessorsBuilder} that consumes message from topic expecting tasks of type
     * which can be parsed by taskExtractor.
     * @param topic the name of topic to consume.
     * @param taskExtractor the extractor to extract task of type {@link T} from message bytes.
     * @param <T> the type of instantiated tasks.
     * @return an instance of {@link ProcessorsBuilder}.
     */
    public static <T> ProcessorsBuilder<T> consuming(String topic, TaskExtractor<T> taskExtractor) {
        // Retry tasks might be stored in retry-topic in DecatonTaskRequest format depending on
        // decaton.task.metadata.as.header configuration.
        // Hence, we need to extract the task with DefaultTaskExtractor to "unwrap" the task first,
        // then extract the task with the given taskExtractor.
        DefaultTaskExtractor<byte[]> outerExtractor = new DefaultTaskExtractor<>(bytes -> bytes);
        TaskExtractor<T> retryTaskExtractor = record -> {
            DecatonTask<byte[]> rawTask = outerExtractor.extract(record);
            ConsumerRecord<byte[], byte[]> inner = new ConsumerRecord<>(
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.timestamp(),
                    record.timestampType(),
                    record.serializedKeySize(),
                    rawTask.taskDataBytes().length,
                    record.key(),
                    rawTask.taskDataBytes(),
                    record.headers(),
                    record.leaderEpoch()
            );
            DecatonTask<T> extracted = taskExtractor.extract(inner);
            return new DecatonTask<>(
                    // Use rawTask#metadata because retry count is stored in rawTask#metada not extracted#metadata
                    rawTask.metadata(),
                    extracted.taskData(),
                    extracted.taskDataBytes());
        };
        return new ProcessorsBuilder<>(topic, taskExtractor, retryTaskExtractor);
    }

    /**
     * An instance of {@link DecatonProcessorSupplier} can be supplied to customize processor instance's
     * creation and destruction.
     * Unless you need to inject special treatment during processor's creation/destruction,
     * {@link #thenProcess(Supplier, ProcessorScope)} or {@link #thenProcess(DecatonProcessor)} should be used
     * instead.
     */
    public ProcessorsBuilder<T> thenProcess(DecatonProcessorSupplier<T> supplier) {
        suppliers.add(supplier);
        return this;
    }

    /**
     * Set a {@link DecatonProcessor} supplier that is used to instantiate {@link DecatonProcessor} used by
     * built subscription.
     * The argument {@link ProcessorScope} controls in which scope should Decaton create a new instance of
     * {@link DecatonProcessor}.
     * It is guaranteed whenever Decaton closes some or all processing scopes, {@link DecatonProcessor#close()}
     * will be called once.
     * @param supplier a {@link Supplier} which returns an instance of {@link DecatonProcessor} when it called.
     * @param scope one of {@link ProcessorScope} which controls when Decaton creates a new instance of
     * {@link DecatonProcessor} calling given supplier.
     * @return updated instance of {@link SubscriptionBuilder}.
     */
    public ProcessorsBuilder<T> thenProcess(Supplier<DecatonProcessor<T>> supplier, ProcessorScope scope) {
        return thenProcess(new DecatonProcessorSupplierImpl<>(supplier, scope));
    }

    /**
     * Set a {@link DecatonProcessor} that is used to process tasks by all partitions.
     * This method will is a syntax sugar of calling {@link #thenProcess(Supplier, ProcessorScope)} with wrapping
     * given processor by {@link Supplier} and {@link ProcessorScope#PROVIDED}.
     * @param processor an instance of {@link DecatonProcessor} which is used to process tasks in built
     * subscription.
     * @return updated instance of {@link SubscriptionBuilder}.
     */
    public ProcessorsBuilder<T> thenProcess(DecatonProcessor<T> processor) {
        return thenProcess(new DecatonProcessorSupplierImpl<>(() -> processor, ProcessorScope.PROVIDED));
    }

    Processors<T> build(DecatonProcessorSupplier<byte[]> retryProcessorSupplier) {
        return new Processors<>(suppliers, retryProcessorSupplier, taskExtractor, retryTaskExtractor);
    }
}
