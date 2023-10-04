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

package com.linecorp.decaton.testing;

import java.util.Random;

import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;

import lombok.Getter;
import lombok.experimental.Accessors;

/**
 * Test rule to provide {@link Random} instance with including
 * random seed info in error message for reproducibility
 */
public class RandomExtension implements BeforeEachCallback, TestWatcher {
    private long seed;
    @Getter
    @Accessors(fluent = true)
    private Random random;

    @Override
    public void testFailed(ExtensionContext context, Throwable cause) {
        throw new RuntimeException(String.format("%s failed. [randomSeed=%d]",
                                                 context.getDisplayName(),
                                                 seed), cause);
    }

    @Override
    public void beforeEach(ExtensionContext extensionContext) throws Exception {
        seed = System.currentTimeMillis();
        random = new Random(seed);
    }
}
