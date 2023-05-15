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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.slf4j.MDC;

import com.linecorp.decaton.processor.Completion;
import com.linecorp.decaton.processor.DecatonProcessor;
import com.linecorp.decaton.processor.ProcessingContext;
import com.linecorp.decaton.processor.runtime.internal.AbstractDecatonProperties;
import com.linecorp.decaton.processor.runtime.internal.OutOfOrderCommitControl;
import com.linecorp.decaton.processor.runtime.internal.RateLimiter;

/**
 * Collection of properties that can be configured to adjust {@link DecatonProcessor}'s behavior.
 *
 * Description of each attributes:
 * - Reloadable : Whether update on the property can be applied to running instance without restarting it.
 *                Note that for properties enabled for this attribute, updating its value may take certain
 *                latency.
 */
public class ProcessorProperties extends AbstractDecatonProperties {
    /**
     * List of keys of task to skip processing.
     *
     * Note that this property accepts only String keys, while Decaton consumer supports consuming
     * keys of arbitrary type. This means that records with non-String keys may just pass through
     * this filter.
     *
     * Reloadable: yes
     */
    public static final PropertyDefinition<List<String>> CONFIG_IGNORE_KEYS =
            PropertyDefinition.define("decaton.ignore.keys", List.class, Collections.emptyList(),
                                      PropertyDefinition.checkListElement(String.class));
    /**
     * Maximum rate of processing tasks per-partition in second.
     *
     * If the value N is
     *   - (0, 1,000,000]: Do the best to process tasks as much as N per second.
     *       N may not be kept well if a task takes over a second to process or N is greater than
     *       actual throughput per second.
     *   -  0: Stop all processing but the task currently being processed isn`t interrupted
     *   - -1: Unlimited
     *
     * See also {@link RateLimiter}.
     *
     * Reloadable: yes
     */
    public static final PropertyDefinition<Long> CONFIG_PROCESSING_RATE =
            PropertyDefinition.define("decaton.processing.rate.per.partition", Long.class,
                                      RateLimiter.UNLIMITED,
                                      v -> v instanceof Long
                                           && RateLimiter.UNLIMITED <= (long) v
                                           && (long) v <= RateLimiter.MAX_RATE);
    /**
     * Concurrency used to process tasks coming from single partition.
     * Reloading this property will be performed for each assigned partition as soon as
     * the current pending tasks of the assigned partition have done.
     *
     * Reloadable: yes
     */
    public static final PropertyDefinition<Integer> CONFIG_PARTITION_CONCURRENCY =
            PropertyDefinition.define("decaton.partition.concurrency", Integer.class, 1,
                                      v -> v instanceof Integer && (Integer) v > 0);
    /**
     * Number of records to pause source partition if pending count exceeds this number.
     *
     * Reloadable: no
     */
    public static final PropertyDefinition<Integer> CONFIG_MAX_PENDING_RECORDS =
            PropertyDefinition.define("decaton.max.pending.records", Integer.class, 10_000,
                                      v -> v instanceof Integer && (Integer) v > 0);
    /**
     * Interval in milliseconds to put in between offset commits.
     * Too frequent offset commit would cause high load on brokers while it doesn't essentially prevents
     * duplicate processing.
     *
     * Reloadable: yes
     */
    public static final PropertyDefinition<Long> CONFIG_COMMIT_INTERVAL_MS =
            PropertyDefinition.define("decaton.commit.interval.ms", Long.class, 1000L,
                                      v -> v instanceof Long && (Long) v >= 0);
    /**
     * Timeout for consumer group rebalance.
     * Decaton waits up to this time for tasks currently pending or in-progress to finish before allowing a
     * rebalance to proceed.
     * Any tasks that do not complete within this timeout will not have their offsets committed before the
     * rebalance, meaning they may be processed multiple times (as they will be processed again after the
     * rebalance). If {@link #CONFIG_PARTITION_CONCURRENCY} is greater than 1, this situation might also cause
     * other records from the same partition to be processed multiple times (see
     * {@link OutOfOrderCommitControl}).
     * Generally, this should be set such that {@link #CONFIG_MAX_PENDING_RECORDS} can be comfortably processed
     * within this timeout.
     *
     * Reloadable: yes
     */
    public static final PropertyDefinition<Long> CONFIG_GROUP_REBALANCE_TIMEOUT_MS =
            PropertyDefinition.define("decaton.group.rebalance.timeout.ms", Long.class, 1000L,
                                      v -> v instanceof Long && (Long) v >= 0);

    /**
     * Timeout for processor close. Decaton waits up to this time for tasks currently pending or in-progress
     * to finish. Any tasks that do not complete within this timeout will mean async task processing code may
     * still be running even after {@link ProcessorSubscription#close()} returns, which might lead to errors
     * from e.g. shutting down dependencies of this {@link ProcessorSubscription} that are still in use from
     * async tasks.
     * Generally, this should be set such that {@link #CONFIG_MAX_PENDING_RECORDS} can be comfortably processed
     * within this timeout.
     *
     * Reloadable: yes
     */
    public static final PropertyDefinition<Long> CONFIG_SHUTDOWN_TIMEOUT_MS =
            PropertyDefinition.define("decaton.processing.shutdown.timeout.ms", Long.class, 0L,
                                      v -> v instanceof Long && (Long) v >= 0);

    /**
     * Control whether to enable or disable decaton specific information store in slf4j's {@link MDC}.
     * This option is enabled by default, but it is known to cause some object allocations which could become
     * a problem in massive scale traffic. This option intend to provide an option for users to disable MDC
     * properties where not necessary to reduce GC pressure.
     *
     * Reloadable: yes
     */
    public static final PropertyDefinition<Boolean> CONFIG_LOGGING_MDC_ENABLED =
            PropertyDefinition.define("decaton.logging.mdc.enabled", Boolean.class, true,
                                      v -> v instanceof Boolean);
    /**
     * Controls whether to enable or disable binding Micrometer's KafkaClientMetrics to decaton consumers.
     * This is disabled for backwards compatiblity, but recommended if you rely on Micrometer
     * since JMX metrics are deprecated. The downside is a possible increase in metrics count.
     *
     * Reloadable: no
     */
    public static final PropertyDefinition<Boolean> CONFIG_BIND_CLIENT_METRICS =
            PropertyDefinition.define("decaton.client.metrics.micrometer.bound", Boolean.class, false,
                                      v -> v instanceof Boolean);
    /**
     * Control time to "timeout" a deferred completion.
     * Decaton allows {@link DecatonProcessor}s to defer completion of a task by calling
     * {@link ProcessingContext#deferCompletion()}, which is useful for processors which integrates with
     * asynchronous processing frameworks that sends the processing context to somewhere else and get back
     * later.
     * However, since leaking {@link Completion} returned by {@link ProcessingContext#deferCompletion()} means
     * to create a never-completed task, that causes consumption to suck completely after
     * {@link #CONFIG_MAX_PENDING_RECORDS} records stacked up, which is not desirable for some use cases.
     *
     * By setting this timeout, Decaton will try to "timeout" a deferred completion after the specified period.
     * By setting the timeout to sufficiently large value, which you can be sure that none of normal processing
     * to take, some potentially leaked completion will be forcefully completed and decaton can continue to
     * consume the following tasks.
     *
     * Be very careful when using this feature since forcefully completing a timed out completion might leads
     * you some data loss if the corresponding processing hasn't yet complete.
     *
     * This timeout can be disabled by setting -1, and it is the default.
     *
     * Reloadable: yes
     */
    public static final PropertyDefinition<Long> CONFIG_DEFERRED_COMPLETE_TIMEOUT_MS =
            PropertyDefinition.define("decaton.deferred.complete.timeout.ms", Long.class, -1L,
                                      v -> v instanceof Long);
    /**
     * Control per-key processing rate per second quota.
     * Only effective when per-key quota is enabled by {@link SubscriptionBuilder#enablePerKeyQuota}.
     * <p>
     * Reloadable: yes
     */
    public static final PropertyDefinition<Long> CONFIG_PER_KEY_QUOTA_PROCESSING_RATE =
            PropertyDefinition.define("decaton.per.key.quota.processing.rate", Long.class,
                                      RateLimiter.UNLIMITED,
                                      v -> v instanceof Long
                                           && RateLimiter.UNLIMITED <= (long) v
                                           && (long) v <= RateLimiter.MAX_RATE);

    /**
     * Timeout for processor threads termination.
     * When a partition is revoked for rebalance or a subscription is about to be shutdown,
     * all processors will be destroyed.
     * At this time, Decaton waits synchronously for the running tasks to finish until this timeout.
     *
     * Even if timeout occurs, Decaton will continue other clean-up tasks.
     * Therefore, you can set this timeout only if unexpected behavior is acceptable in the middle of the last
     * {@link DecatonProcessor#process(ProcessingContext, Object)} which timed out.
     *
     * Reloadable: yes
     */
    public static final PropertyDefinition<Long> CONFIG_PROCESSOR_THREADS_TERMINATION_TIMEOUT_MS =
            PropertyDefinition.define("decaton.processor.threads.termination.timeout.ms", Long.class,
                                      Long.MAX_VALUE, v -> v instanceof Long && (Long) v >= 0);

    public static final List<PropertyDefinition<?>> PROPERTY_DEFINITIONS =
            Collections.unmodifiableList(Arrays.asList(
                    CONFIG_IGNORE_KEYS,
                    CONFIG_PROCESSING_RATE,
                    CONFIG_PARTITION_CONCURRENCY,
                    CONFIG_MAX_PENDING_RECORDS,
                    CONFIG_COMMIT_INTERVAL_MS,
                    CONFIG_GROUP_REBALANCE_TIMEOUT_MS,
                    CONFIG_SHUTDOWN_TIMEOUT_MS,
                    CONFIG_LOGGING_MDC_ENABLED,
                    CONFIG_BIND_CLIENT_METRICS,
                    CONFIG_DEFERRED_COMPLETE_TIMEOUT_MS,
                    CONFIG_PROCESSOR_THREADS_TERMINATION_TIMEOUT_MS,
                    CONFIG_PER_KEY_QUOTA_PROCESSING_RATE));

    /**
     * Find and return a {@link PropertyDefinition} from its name.
     * This method is not optimized for frequent invocation.
     *
     * @param name the name of property.
     * @return a {@link PropertyDefinition} instance.
     */
    public static PropertyDefinition<?> definitionForName(String name) {
        for (PropertyDefinition<?> def : PROPERTY_DEFINITIONS) {
            if (def.name().equals(name)) {
                return def;
            }
        }
        throw new IllegalArgumentException("no such property definition: " + name);
    }

    /**
     * Creates a new {@link Property} from the given name and value.
     * This might be useful when code for constructing {@link ProcessorProperties} just needs to bridge
     * a map of property name and values to {@link Property} instances.
     * This method is not optimized for frequent invocation.
     *
     * @param name the name of property.
     * @param value the value to assign for the property.
     * @return a {@link Property} instance.
     * @throws IllegalArgumentException if given name is not present in definitions.
     */
    @SuppressWarnings("unchecked")
    public static Property<?> propertyForName(String name, Object value) {
        PropertyDefinition<Object> def = (PropertyDefinition<Object>) definitionForName(name);
        return Property.ofStatic(def, value);
    }

    public static Builder<ProcessorProperties> builder() {
        return new Builder<>(
                ProcessorProperties::new,
                PROPERTY_DEFINITIONS);
    }

    public ProcessorProperties(Map<PropertyDefinition<?>, Property<?>> properties) {
        super(properties);
    }

    /**
     * Returns a List of properties with default values.
     */
    public static List<Property<?>> defaultProperties() {
        List<Property<?>> properties = new ArrayList<>();
        PROPERTY_DEFINITIONS.forEach(definition -> properties.add(Property.ofStatic(definition)));
        return properties;
    }
}
