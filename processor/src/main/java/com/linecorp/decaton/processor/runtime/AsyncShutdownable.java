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

import java.time.Duration;
import java.util.concurrent.ExecutorService;

/**
 * Represents a service with a two-stage shutdown process, vaguely analogous to that of {@link ExecutorService}:
 * allows us to initiate a shutdown that will proceed asynchronously and then subsequently await the completion
 * of that asynchronous process. This can be useful e.g. when terminating several {@link ProcessorSubscription}s
 * at once (e.g. for application shutdown) where we would like to "gracefully" shut down by allowing each
 * subscription to finish processing all "in-flight" tasks, but would also like to shut down within a reasonable
 * time frame: we can first call initiateShutdown on each instance, and then call awaitShutdown on each.
 * Although the worst-case time is the same as simply calling close on each instance in turn, in practice this
 * will usually lead to a quicker overall shutdown process.
 */
public interface AsyncShutdownable extends AutoCloseable {
    /**
     * Start the shutdown process but return without blocking.
     * Actual shutdown may be ongoing asynchronously after this method returns.
     * Use {@link #awaitShutdown()} to wait for shutdown to complete.
     */
    void initiateShutdown();

    /**
     * Block until shutdown completes, the given duration limit has passed, or this thread is interrupted.
     * @param limit maximum time to block for
     * @throws InterruptedException if this thread is interrupted
     * @return true if shutdown completed, false if the limit elapsed before shutdown completed.
     */
    boolean awaitShutdown(Duration limit) throws InterruptedException;

    /**
     * Block until shutdown completes or this thread is interrupted
     */
    default void awaitShutdown() throws InterruptedException {
        awaitShutdown(Duration.ofMillis(Long.MAX_VALUE));
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
