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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linecorp.decaton.processor.Property;

class DynamicRateLimiter implements RateLimiter {
    private static final Logger logger = LoggerFactory.getLogger(DynamicRateLimiter.class);

    private volatile RateLimiter current;

    DynamicRateLimiter(Property<Long> rateProperty) {
        current = createLimiter(RateLimiter.UNLIMITED);

        rateProperty.listen((oldValue, newValue) -> {
            RateLimiter oldLimiter = current;
            current = createLimiter(newValue);

            try {
                oldLimiter.close();
            } catch (Exception e) {
                logger.warn("Failed to close rate limiter: {}", oldLimiter, e);
            }
        });
    }

    // visible for testing
    RateLimiter createLimiter(long permitsPerSecond) {
        return RateLimiter.create(permitsPerSecond);
    }

    @Override
    public long acquire(int permits) throws InterruptedException {
        return current.acquire(permits);
    }

    @Override
    public void close() throws Exception {
        current.close();
    }
}
