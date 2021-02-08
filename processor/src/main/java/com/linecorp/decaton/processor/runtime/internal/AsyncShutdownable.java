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

package com.linecorp.decaton.processor.runtime.internal;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

public interface AsyncShutdownable extends AutoCloseable {
    /**
     * Start the shutdown process but return without blocking.
     * Actual shutdown may be ongoing asynchronously after this method returns.
     * Use {@link #awaitShutdown()} to wait for shutdown to complete.
     */
    void initiateShutdown();

    /**
     * Block until shutdown completes, the given duration limit has passed,
     * or this thread is interrupted.
     */
    void awaitShutdown(Duration limit) throws InterruptedException;

    /**
     * Block until shutdown completes or this thread is interrupted
     */
    default void awaitShutdown() throws InterruptedException {
        awaitShutdown(ChronoUnit.FOREVER.getDuration());
    }

    /**
     * Shut down, blocking until shutdown is complete
     */
    @Override
    default void close() throws Exception {
        initiateShutdown();
        awaitShutdown();
    }
}
