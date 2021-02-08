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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.linecorp.decaton.processor.DecatonProcessor;
import com.linecorp.decaton.processor.runtime.internal.AbstractDecatonProperties;
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
     * Reloading this property will pause all assigned partitions until current pending tasks have done.
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
            PropertyDefinition.define("decaton.max.pending.records", Integer.class, 100,
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
     * Decaton waits up to this time for tasks currently in-progress to finish and then start rebalancing
     * operations.
     * Failing to join on tasks completion within this timeout would cause offset commit to not to happen
     * which might consequence duplicate processing.
     *
     * Reloadable: yes
     */
    public static final PropertyDefinition<Long> CONFIG_GROUP_REBALANCE_TIMEOUT_MS =
            PropertyDefinition.define("decaton.group.rebalance.timeout.ms", Long.class, 1000L,
                                      v -> v instanceof Long && (Long) v >= 0);

    /**
     * Timeout for processor close. Decaton waits up to this time for tasks currently in-progress to finish.
     * Any tasks that do not complete within this timeout will mean async task processing code may still
     * be running even after {@link ProcessorSubscription#close()} returns, which might lead to errors from e.g.
     * shutting down dependencies of this {@link ProcessorSubscription} that are still in use from async tasks.
     * Reloadable: yes
     */
    public static final PropertyDefinition<Long> CONFIG_CLOSE_TIMEOUT_MS =
            PropertyDefinition.define("decaton.processing.shutdown.timeout.ms", Long.class, 0L,
                                      v -> v instanceof Long && (Long) v >= 0);

    public static final List<PropertyDefinition<?>> PROPERTY_DEFINITIONS =
            Collections.unmodifiableList(Arrays.asList(
                    CONFIG_IGNORE_KEYS,
                    CONFIG_PROCESSING_RATE,
                    CONFIG_PARTITION_CONCURRENCY,
                    CONFIG_MAX_PENDING_RECORDS,
                    CONFIG_COMMIT_INTERVAL_MS,
                    CONFIG_GROUP_REBALANCE_TIMEOUT_MS,
                    CONFIG_CLOSE_TIMEOUT_MS));

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
}
