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

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.function.LongSupplier;

import org.apache.kafka.common.TopicPartition;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import com.linecorp.decaton.processor.runtime.DefaultSubPartitioner;
import com.linecorp.decaton.processor.runtime.PerKeyQuotaConfig;
import com.linecorp.decaton.processor.runtime.ProcessorProperties;
import com.linecorp.decaton.processor.runtime.Property;
import com.linecorp.decaton.processor.runtime.internal.AbstractDecatonProperties.Builder;
import com.linecorp.decaton.processor.runtime.internal.PerKeyQuotaManager.QuotaUsage;
import com.linecorp.decaton.processor.runtime.internal.PerKeyQuotaManager.UsageType;
import com.linecorp.decaton.processor.runtime.internal.WindowedKeyStat.Stat;
import com.linecorp.decaton.processor.tracing.internal.NoopTracingProvider;

public class PerKeyQuotaManagerTest {
    private final byte[] key = "key".getBytes(StandardCharsets.UTF_8);
    private final long timestamp = 1677420878461L;

    @Rule
    public MockitoRule rule = MockitoJUnit.rule();

    @Mock
    private LongSupplier timestampSupplier;

    @Mock
    private WindowedKeyStat windowedStat;

    private static PartitionScope scope(long processingRateQuota) {
        Builder<ProcessorProperties> builder = ProcessorProperties.builder();
        builder.set(Property.ofStatic(ProcessorProperties.CONFIG_PER_KEY_QUOTA_PROCESSING_RATE, processingRateQuota));
        return new PartitionScope(
                new SubscriptionScope("subscription", "topic",
                                      Optional.empty(), Optional.of(PerKeyQuotaConfig.shape()),
                                      builder.build(),
                                      NoopTracingProvider.INSTANCE,
                                      ConsumerSupplier.DEFAULT_MAX_POLL_RECORDS,
                                      DefaultSubPartitioner::new),
                new TopicPartition("topic", 0));
    }

    @Test
    public void testUnlimited() {
        PerKeyQuotaManager manager = new PerKeyQuotaManager(
                scope(RateLimiter.UNLIMITED), timestampSupplier, windowedStat);

        assertEquals(QuotaUsage.COMPLY, manager.record(key));
        verify(windowedStat, never()).recordAndGet(anyLong(), any());
    }

    @Test
    public void testComply() {
        PerKeyQuotaManager manager = new PerKeyQuotaManager(
                scope(42L), timestampSupplier, windowedStat);

        doReturn(timestamp).when(timestampSupplier).getAsLong();
        // 615 / 15 sec = 41 which is under quota
        doReturn(new Stat(timestamp - 15000L, timestamp, 615)).when(windowedStat).recordAndGet(eq(timestamp), eq(key));

        QuotaUsage usage = manager.record(key);
        assertEquals(QuotaUsage.COMPLY, usage);
    }

    @Test
    public void testViolate() {
        PerKeyQuotaManager manager = new PerKeyQuotaManager(
                scope(42L), timestampSupplier, windowedStat);

        doReturn(timestamp).when(timestampSupplier).getAsLong();
        // 645 / 15 sec = 43 which is over quota
        doReturn(new Stat(timestamp - 15000L, timestamp, 645)).when(windowedStat).recordAndGet(eq(timestamp), eq(key));

        QuotaUsage usage = manager.record(key);
        assertEquals(UsageType.VIOLATE, usage.type());
        double epsilon = 0.0001;
        assertEquals(43.0, usage.metrics().rate(), epsilon);
    }

    @Test
    public void testDurationNotEnough() {
        PerKeyQuotaManager manager = new PerKeyQuotaManager(
                scope(42L), timestampSupplier, windowedStat);

        doReturn(timestamp).when(timestampSupplier).getAsLong();
        // 387 / 9 sec = 43 which is over quota but the observed duration is shorter than window size (10 sec)
        doReturn(new Stat(timestamp - 9000L, timestamp, 387)).when(windowedStat).recordAndGet(eq(timestamp), eq(key));

        QuotaUsage usage = manager.record(key);
        assertEquals(QuotaUsage.COMPLY, usage);
    }

    @Test
    public void testNullKey() {
        PerKeyQuotaManager manager = new PerKeyQuotaManager(
                scope(42L), timestampSupplier, windowedStat);

        assertEquals(QuotaUsage.COMPLY, manager.record(null));
        verify(windowedStat, never()).recordAndGet(anyLong(), any());
    }
}
