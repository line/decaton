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
import java.util.function.Function;
import java.util.function.Supplier;

import com.linecorp.decaton.common.Deserializer;
import com.linecorp.decaton.processor.DecatonProcessor;
import com.linecorp.decaton.processor.TaskMetadata;
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
    private final Function<Property<Boolean>, TaskExtractor<T>> taskExtractorConstructor;
    private final Function<Property<Boolean>, TaskExtractor<T>> retryTaskExtractorConstructor;

    private final List<DecatonProcessorSupplier<T>> suppliers;

    ProcessorsBuilder(String topic,
                      Function<Property<Boolean>, TaskExtractor<T>> taskExtractorConstructor,
                      Function<Property<Boolean>, TaskExtractor<T>> retryTaskExtractorConstructor) {
        this.topic = topic;
        this.taskExtractorConstructor = taskExtractorConstructor;
        this.retryTaskExtractorConstructor = retryTaskExtractorConstructor;
        suppliers = new ArrayList<>();
    }

    /**
     * Create new {@link ProcessorsBuilder} that consumes message from topic expecting tasks of type
     * which can be parsed by deserializer.
     * <p>
     * From Decaton 9.0.0, you can use this overload to consume tasks from arbitrary topics not only
     * topics that are produced by DecatonClient.
     * <p>
     * If you want to extract custom {@link TaskMetadata} (e.g. for delayed processing), you can use
     * {@link #consuming(String, TaskExtractor)} instead.
     * @param topic the name of topic to consume.
     * @param deserializer the deserializer to instantiate task of type {@link T} from serialized bytes.
     * @param <T> the type of instantiated tasks.
     * @return an instance of {@link ProcessorsBuilder}.
     */
    public static <T> ProcessorsBuilder<T> consuming(String topic, Deserializer<T> deserializer) {
        Function<Property<Boolean>, TaskExtractor<T>> constructor = prop -> new DefaultTaskExtractor<>(deserializer, prop);
        return new ProcessorsBuilder<>(topic, constructor, constructor);
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
        return new ProcessorsBuilder<>(topic,
                                       ignored -> taskExtractor,
                                       prop -> new RetryTaskExtractor<>(prop, taskExtractor));
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

    Processors<T> build(DecatonProcessorSupplier<byte[]> retryProcessorSupplier, ProcessorProperties properties) {
        Property<Boolean> legacyFallbackEnabledProperty = properties.get(ProcessorProperties.CONFIG_LEGACY_PARSE_FALLBACK_ENABLED);
        return new Processors<>(suppliers,
                                retryProcessorSupplier,
                                taskExtractorConstructor.apply(legacyFallbackEnabledProperty),
                                retryTaskExtractorConstructor.apply(legacyFallbackEnabledProperty));
    }

    private static class RetryTaskExtractor<T> implements TaskExtractor<T> {
        private final DefaultTaskExtractor<byte[]> outerExtractor;
        private final TaskExtractor<T> innerExtractor;

        RetryTaskExtractor(Property<Boolean> legacyFallbackEnabledProperty,
                           TaskExtractor<T> innerExtractor) {
            this.innerExtractor = innerExtractor;
            this.outerExtractor = new DefaultTaskExtractor<>(bytes -> bytes, legacyFallbackEnabledProperty);
        }

        @Override
        public DecatonTask<T> extract(ConsumedRecord record) {
            // Retry tasks might be stored in retry-topic in DecatonTaskRequest format depending on
            // decaton.task.metadata.as.header configuration.
            // Hence, we need to extract the task with DefaultTaskExtractor to "unwrap" the task first,
            // then extract the task with the given taskExtractor.
            DecatonTask<byte[]> outerTask = outerExtractor.extract(record);
            ConsumedRecord inner = ConsumedRecord
                    .builder()
                    .recordTimestampMillis(record.recordTimestampMillis())
                    .headers(record.headers())
                    .key(record.key())
                    .value(outerTask.taskDataBytes())
                    .build();
            DecatonTask<T> extracted = innerExtractor.extract(inner);
            return new DecatonTask<>(
                    // Use outerTask#metadata because retry count is stored in rawTask#metada not extracted#metadata
                    outerTask.metadata(),
                    extracted.taskData(),
                    extracted.taskDataBytes());
        }
    }
}
