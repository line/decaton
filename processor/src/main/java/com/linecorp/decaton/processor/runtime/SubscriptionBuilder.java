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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import com.linecorp.decaton.client.DecatonClientBuilder.DefaultKafkaProducerSupplier;
import com.linecorp.decaton.client.internal.DecatonTaskProducer;
import com.linecorp.decaton.client.KafkaProducerSupplier;
import com.linecorp.decaton.processor.DecatonProcessor;
import com.linecorp.decaton.processor.ProcessingContext;
import com.linecorp.decaton.processor.TaskMetadata;
import com.linecorp.decaton.processor.runtime.internal.NoopQuotaApplier;
import com.linecorp.decaton.processor.runtime.internal.QuotaApplier;
import com.linecorp.decaton.processor.runtime.internal.QuotaApplierImpl;
import com.linecorp.decaton.processor.tracing.TracingProvider;
import com.linecorp.decaton.processor.runtime.internal.ConsumerSupplier;
import com.linecorp.decaton.processor.runtime.internal.DecatonProcessorSupplierImpl;
import com.linecorp.decaton.processor.runtime.internal.DecatonTaskRetryQueueingProcessor;
import com.linecorp.decaton.processor.runtime.internal.SubscriptionScope;
import com.linecorp.decaton.processor.tracing.internal.NoopTracingProvider;

import lombok.AccessLevel;
import lombok.Setter;
import lombok.experimental.Accessors;

@Setter
@Accessors(fluent = true)
public class SubscriptionBuilder {
    private static final Map<String, String> presetRetryProducerConfig;
    private static final Map<String, String> presetShapingProducerConfig;

    static {
        presetRetryProducerConfig = new HashMap<>();
        // In Decaton processor which handles massive traffic, retry tasks could cause
        // production burst when processes start to fail. (e.g. due to downstream service down)
        // Since default producer's linger.ms is 0, this could harm Kafka cluster despite we
        // don't much care about retry task's delivery latency typically. So we set reasonable default here.
        presetRetryProducerConfig.put(ProducerConfig.LINGER_MS_CONFIG, "100");

        presetShapingProducerConfig = new HashMap<>();
        // This producer is used to produce tasks for bursting keys, so this producer must be
        // efficient so that shaping itself won't be the bottleneck.
        presetShapingProducerConfig.put(ProducerConfig.LINGER_MS_CONFIG, "100");
    }

    @Setter(AccessLevel.NONE)
    private ProcessorProperties.Builder<ProcessorProperties> propertiesBuilder;

    /**
     * A unique identifier for this subscription. This ID is used mainly for identifying logs, metrics and
     * threads associated to this subscription, and is not used for any purpose to interact with Kafka brokers.
     */
    private final String subscriptionId;
    /**
     * Properties storing configuration for {@link Consumer} to be created and used.
     * See {@link ConsumerConfig} for the possible tunables.
     * Decaton may overwrite some values for making sure consumers to behave in expected way.
     * See {@link ConsumerSupplier}.
     */
    private Properties consumerConfig;
    /**
     * A {@link ProcessorsBuilder} to configure processors pipeline which actually processes tasks.
     */
    private ProcessorsBuilder<?> processorsBuilder;
    /**
     * A {@link SubscriptionStateListener} to listen state changes of {@link ProcessorSubscription}
     */
    private SubscriptionStateListener stateListener;

    @Setter(AccessLevel.NONE)
    private RetryConfig retryConfig;

    @Setter(AccessLevel.NONE)
    private PerKeyQuotaConfig perKeyQuotaConfig;

    /**
     * A {@link TracingProvider} for tracing the execution of record processing
     */
    private TracingProvider tracingProvider = NoopTracingProvider.INSTANCE;

    /**
     *
     * A {@link SubPartitionerSupplier} for partitioning tasks into subpartitions
     */
    @Setter
    private SubPartitionerSupplier subPartitionerSupplier = DefaultSubPartitioner::new;

    public SubscriptionBuilder(String subscriptionId) {
        this.subscriptionId = Objects.requireNonNull(subscriptionId, "subscriptionId");
        propertiesBuilder = ProcessorProperties.builder();
    }

    public static SubscriptionBuilder newBuilder(String subscriptionId) {
        return new SubscriptionBuilder(subscriptionId);
    }

    /**
     * Configure Decaton processor tunables that are defined in {@link ProcessorProperties}.
     *
     * Users can pass multiple {@link PropertySupplier} implementations which might provides partial set of
     * properties.
     * If more than one supplier provides the same property, THE FIRST ONE BEATS (in order of arguments).
     *
     * @param suppliers {@link PropertySupplier} instances
     * @return updated instance of {@link SubscriptionBuilder}.
     */
    public SubscriptionBuilder addProperties(PropertySupplier... suppliers) {
        for (PropertySupplier supplier : suppliers) {
            propertiesBuilder.setBySupplier(supplier);
        }
        return this;
    }

    /**
     * Resets previous invocations of {@link #properties(PropertySupplier...)}
     * and applies {@link #addProperties(PropertySupplier...)}.
     */
    public SubscriptionBuilder properties(PropertySupplier... suppliers) {
        propertiesBuilder = ProcessorProperties.builder();
        return addProperties(suppliers);
    }

    /**
     * Configure subscription to enable retry processing when {@link DecatonProcessor} requests.
     *
     * Some prerequisites needs to be confirm in order to enable retry function:
     * - For re-queueing tasks for retry, an another topic needs to be prepared on the same Kafka cluster.
     *   The topic should be named as {@link ProcessorsBuilder#topic()} + "-retry" or customized through
     *   {@link RetryConfig#retryTopic()}.
     * - By enabling retry, processing order of tasks is no longer guaranteed. When task1 of particular key
     *   scheduled for retrying, and task2 of the same key arrived before task1 gets processed again, task2
     *   processing might be completed in prior to task1's.
     * - Configuration parameter {@link RetryConfig#backoff()} configures "minimum" amount of time to backoff
     *   before attempting to process task again. Hence it guarantees task scheduled for retry at time T to be
     *   processed at least later than T + {@link RetryConfig#backoff()}, but it does not guarantee that the
     *   task is to be processed exactly at T + {@link RetryConfig#backoff()}.
     *
     * The number of attempts to retry processing is controlled by {@link DecatonProcessor}, by deciding
     * whether to call {@link ProcessingContext#retry()}, maybe accordingly to
     * {@link TaskMetadata#retryCount()} for reference how many times did the task attempted to be processed.
     *
     * @param config a {@link RetryConfig} instance representing configs for retry.
     * @return updated instance of {@link SubscriptionBuilder}.
     */
    public SubscriptionBuilder enableRetry(RetryConfig config) {
        retryConfig = config;
        return this;
    }

    /**
     * Configure the subscription to enable per-key quota to prevent bursting keys occupy the resources
     * and causing process for other keys to delay.
     * <p>
     * Decaton adopts "shaping" strategy for those bursting keys.
     * Once a key is detected as bursting, Decaton will queue tasks for this key to the shaping topic and immediately complete it.
     * <p>
     * Some prerequisites need to be confirmed in order to use per-key quota feature:
     * - For shaping tasks queueing, shaping topics need to be prepared on the same Kafka cluster.
     *   Unless you supply custom shaping topics and callback, the topic name should be {@link ProcessorsBuilder#topic()} + "-shaping".
     * - By enabling shaping, per-key serial processing of tasks is no longer guaranteed.
     * - Decaton adopts probabilistic algorithm to account per-key processing rate.
     *   Currently, the algorithm allows "false-positives" so some keys can be detected as burst
     *   even when its actual processing rate is not excessive.
     * - Rate-limit will be applied for shaping topics to process bursting tasks without excessive resource usage.
     *   {@link ProcessorProperties#CONFIG_PER_KEY_QUOTA_PROCESSING_RATE} will be used as the rate-limit value by default.
     *   If you need to set rate-limit value per shaping-topic, you can override it via {@link SubscriptionBuilder#overrideShapingRate}.
     * - Per-key quota doesn't apply to the retry-topic.
     *
     * @param config a {@link PerKeyQuotaConfig} instance representing configs.
     * @return updated instance of {@link SubscriptionBuilder}.
     */
    public SubscriptionBuilder enablePerKeyQuota(PerKeyQuotaConfig config) {
        perKeyQuotaConfig = config;
        return this;
    }

    /**
     * Optionally configure processing rate per-shaping topic using the {@link PropertySupplier}.
     * @param shapingTopic shaping topic to override the processing rate
     * @param supplier {@link PropertySupplier} instance to configure get the processing rate property
     * @return updated instance of {@link SubscriptionBuilder}.
     */
    public SubscriptionBuilder overrideShapingRate(String shapingTopic, PropertySupplier supplier) {
        PropertyDefinition<Long> definition = PerKeyQuotaConfig.shapingRateProperty(shapingTopic);
        Optional<Property<Long>> property = supplier.getProperty(definition);
        propertiesBuilder.set(
                property.orElseThrow(() -> new IllegalArgumentException(definition.name() + " must exist in the supplier")));
        propertiesBuilder.set(property.get());
        return this;
    }

    /**
     * @param tracingProvider Add tracing provider that will be called to trace the execution of processing
     * each record.
     * @return updated instance of {@link SubscriptionBuilder}.
     */
    public SubscriptionBuilder enableTracing(TracingProvider tracingProvider) {
        this.tracingProvider = Objects.requireNonNull(tracingProvider, "tracingProvider must not be null");
        return this;
    }

    private int consumerMaxPollRecords() {
        if (consumerConfig.getProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG) != null) {
            return Integer.parseInt(consumerConfig.getProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG));
        } else {
            return ConsumerSupplier.DEFAULT_MAX_POLL_RECORDS;
        }
    }

    public ProcessorSubscription build() {
        ProcessorProperties props = propertiesBuilder.build();
        String topic = processorsBuilder.topic();
        SubscriptionScope scope = new SubscriptionScope(Objects.requireNonNull(subscriptionId),
                                                        topic,
                                                        Optional.ofNullable(retryConfig),
                                                        Optional.ofNullable(perKeyQuotaConfig),
                                                        props,
                                                        tracingProvider,
                                                        consumerMaxPollRecords(),
                                                        subPartitionerSupplier);

        Properties consumerConfig = Objects.requireNonNull(this.consumerConfig, "consumerConfig");
        ConsumerSupplier consumerSupplier = new ConsumerSupplier(consumerConfig);

        return new ProcessorSubscription(scope,
                                         consumerSupplier.get(),
                                         quotaApplier(scope),
                                         processorsBuilder.build(maybeRetryProcessorSupplier(scope)),
                                         props,
                                         stateListener);
    }

    public ProcessorSubscription buildAndStart() {
        ProcessorSubscription subscription = build();
        subscription.start();
        return subscription;
    }

    /**
     * Creates a supplier to get {@link Properties} for a kafka producer by taking an intersection
     * of {@link ProducerConfig#configNames)} and the given {@link Properties} of the consumer.
     *
     */
    static Supplier<Properties> producerConfigSupplier(Properties consumerConfig) {
        return () -> {
            Properties producerProps = new Properties();
            Set<String> definedProps = ProducerConfig.configNames();
            consumerConfig.stringPropertyNames()
                          .stream()
                          .filter(definedProps::contains)
                          .forEach(propertyName -> producerProps.setProperty(
                                  propertyName, consumerConfig.getProperty(propertyName)));
            return producerProps;
        };
    }

    private DecatonProcessorSupplier<byte[]> maybeRetryProcessorSupplier(SubscriptionScope scope) {
        if (retryConfig == null) {
            return null;
        }
        Properties producerConfig = new Properties();
        producerConfig.putAll(presetRetryProducerConfig);
        producerConfig.putAll(Optional.ofNullable(retryConfig.producerConfig())
                                      .orElseGet(producerConfigSupplier(consumerConfig)));
        KafkaProducerSupplier producerSupplier = Optional.ofNullable(retryConfig.producerSupplier())
                                                         .orElseGet(DefaultKafkaProducerSupplier::new);
        return new DecatonProcessorSupplierImpl<>(() -> {
            DecatonTaskProducer producer = new DecatonTaskProducer(scope.retryTopic().get(), producerConfig, producerSupplier);
            return new DecatonTaskRetryQueueingProcessor(scope, producer);
        }, ProcessorScope.SINGLETON);
    }

    private QuotaApplier quotaApplier(SubscriptionScope scope) {
        if (perKeyQuotaConfig == null) {
            return NoopQuotaApplier.INSTANCE;
        }
        Properties producerConfig = new Properties();
        producerConfig.putAll(presetShapingProducerConfig);
        producerConfig.putAll(Optional.ofNullable(perKeyQuotaConfig.producerConfig())
                                      .orElseGet(producerConfigSupplier(consumerConfig)));
        Function<Properties, Producer<byte[], byte[]>> producerSupplier =
                Optional.ofNullable(perKeyQuotaConfig.producerSupplier())
                        .orElseGet(() -> properties -> new KafkaProducer<>
                                (properties, new ByteArraySerializer(), new ByteArraySerializer()));
        return new QuotaApplierImpl(
                producerSupplier.apply(producerConfig),
                perKeyQuotaConfig.callbackSupplier().apply(scope.topic()),
                scope);
    }
}
