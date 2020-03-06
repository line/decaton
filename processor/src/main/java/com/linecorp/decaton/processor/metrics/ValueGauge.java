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

package com.linecorp.decaton.processor.metrics;

import java.util.concurrent.atomic.AtomicInteger;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * Provides convenient interface for {@link Gauge} by holding underlying AtomicInteger.
 *
 * <p>In the first place, Micrometer recommends not to set Gauge value, to observe target value instead.
 * That kind of pattern is called 'heisen-gauge'. (https://micrometer.io/docs/concepts#_gauges)
 * <p>The reason why we introduce this class against 'heisen-gauge' principle is, once we adopt heisen-gauge
 * we have to maintain a lot of concurrent-access aware fields which might be a bit risky to take just for the metrics.
 */
public class ValueGauge {
    private final AtomicInteger underlying;

    private ValueGauge(AtomicInteger value) {
        this.underlying = value;
    }

    public void increment() {
        underlying.incrementAndGet();
    }

    public void decrement() {
        underlying.decrementAndGet();
    }

    public void set(int value) {
        underlying.set(value);
    }

    public static Builder builder(String name) {
        return new Builder(name);
    }

    @Setter
    @Accessors(fluent = true, chain = true)
    @RequiredArgsConstructor
    public static class Builder {
        private final String name;

        private String description;
        private String baseUnit;
        private Tags tags;

        public ValueGauge register(MeterRegistry registry) {
            AtomicInteger underlying = new AtomicInteger(0);
            Gauge.builder(name, underlying::get)
                 .description(description)
                 .baseUnit(baseUnit)
                 .tags(tags)
                 .register(registry);
            return new ValueGauge(underlying);
        }
    }
}
