/*
 * Copyright 2023 LINE Corporation
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

import java.util.Random;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import com.linecorp.decaton.processor.runtime.PerKeyQuotaConfig.QuotaCallback.Metrics;
import com.linecorp.decaton.processor.runtime.ProcessorProperties;
import com.linecorp.decaton.processor.runtime.Property;
import com.linecorp.decaton.processor.runtime.internal.WindowedKeyStat.Stat;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PerKeyQuotaManager {
    /*
     * These count-min parameters are determined to achieve
     * sufficient accuracy for most use cases with reasonable memory footprint.
     * Let's assume following situation:
     * - We process 1M tasks per partition / sec (= 10M tasks / 10 sec in default per-key-quota window size).
     * - We want to detect top 1% bursting keys (i.e. keys with 10K tasks / sec)
     * - With ε = 0.00005, error factor calculates to 10M * 0.00005 = 500, which is sufficiently smaller than 10K so it's tolerable error.
     * - With δ = 0.00001, the estimation is in error factor at 99.999% probability, which is sufficiently high.
     *
     * Memory footprint is 12MB. Calculation:
     * - Count-min width = 65536 (first power of two larger than e / ε)
     * - Count-min depth = 12 (ceil(ln(1 / δ)))
     * - 65536 * 12 * 8 (long) * 2 (num windows) = 12MB
     */
    private static final double EPSILON = 0.00005;
    private static final double DELTA = 0.00001;

    private static volatile double epsilon = EPSILON;
    private static volatile double delta = DELTA;

    enum UsageType {
        COMPLY,
        VIOLATE,
    }
    @Getter
    @Accessors(fluent = true)
    @RequiredArgsConstructor
    @EqualsAndHashCode
    static class QuotaUsage {
        static final QuotaUsage COMPLY = new QuotaUsage(UsageType.COMPLY, null);

        private final UsageType type;
        private final Metrics metrics;
    }

    private final WindowedKeyStat windowedStat;
    private final LongSupplier timestampSupplier;
    private final Property<Long> processingRate;
    private final long windowMs;

    PerKeyQuotaManager(PartitionScope scope,
                       LongSupplier timestampSupplier,
                       WindowedKeyStat windowedStat) {
        this.timestampSupplier = timestampSupplier;
        this.windowedStat = windowedStat;
        processingRate = scope.props().get(ProcessorProperties.CONFIG_PER_KEY_QUOTA_PROCESSING_RATE);
        windowMs = scope.perKeyQuotaConfig().get().window().toMillis();
    }

    /**
     * Set parameters to adjust the accuracy of Count-Min sketch, which {@link KeyCounter} internally uses.
     * If default parameters don't fit your use case, you can adjust them by calling this method.
     */
    public static void setCountMinParameters(double epsilon, double delta) {
        PerKeyQuotaManager.epsilon = epsilon;
        PerKeyQuotaManager.delta = delta;
    }

    public static PerKeyQuotaManager create(PartitionScope scope) {
        long seed = System.currentTimeMillis();
        log.info("Creating key counters with seed {} for partition {}", seed, scope.topicPartition());

        // All counters should be instantiated from same random seed to
        // generate same hash function family so that calculated hashes can be reused
        // across multiple windows
        Supplier<KeyCounter> counterSupplier = () -> new KeyCounter(new Random(seed), epsilon, delta);

        WindowedKeyStat windowedStat = new WindowedKeyStat(
                scope.perKeyQuotaConfig().get().window(),
                counterSupplier);
        return new PerKeyQuotaManager(scope, System::currentTimeMillis, windowedStat);
    }

    /**
     * Record the key occurrence and return if the quota is violated or compliant for the key.
     */
    public QuotaUsage record(byte[] key) {
        long quota = processingRate.value();
        // Fast path.
        // If the key is null or current quota value is unlimited, return immediately without recording
        if (key == null || quota == RateLimiter.UNLIMITED) {
            return QuotaUsage.COMPLY;
        }

        Stat stat = windowedStat.recordAndGet(timestampSupplier.getAsLong(), key);
        long duration = stat.toMs() - stat.fromMs();
        // If recorded duration is not enough yet (e.g. right after startup),  we wait for a while
        if (duration < windowMs) {
            return QuotaUsage.COMPLY;
        }

        double rate = Math.max(0, (double) stat.count() / duration) * 1000;

        if (rate < quota) {
            return QuotaUsage.COMPLY;
        }
        return new QuotaUsage(UsageType.VIOLATE,
                              Metrics.builder()
                                     .rate(rate).build());
    }
}
