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

package com.linecorp.decaton.processor;

@FunctionalInterface
public interface DecatonProcessor<T> extends AutoCloseable {
    /**
     * Process a given task.
     * No matter this method returns normally or exceptionally, upper frame considers processing has been
     * finished and commit associated offset.
     * If the processing logic needs to be asynchronous and wants to continue processing the next task without
     * waiting completion of the preceding task's completion, the implementation can call
     * {@link ProcessingContext#deferCompletion()} to tell that it needs to defer completion of the processing.
     * Whenever {@link ProcessingContext#deferCompletion()} called, the
     * {@link #process} implementation is responsible for calling {@link DeferredCompletion#complete()} no matter the
     * processing finished successfully or failed.
     * If {@link ProcessingContext#deferCompletion()} hasn't called at the time this method returns, upper
     * frame completes the offset implicitly so the implementation doesn't need to call it explicitly.
     * When this method throw an exception, upper frame logs it as uncaught exception and continues to
     * process the next record. However, if this method throws {@link InterruptedException} during shutdown
     * sequence, currently processed offset won't be committed.
     *
     * @param context a {@link ProcessingContext} which explains the context of which the task being processed.
     * @param task a task object which contains task data to be processed.
     *
     * @throws InterruptedException this method can throw {@link InterruptedException} if it got interrupted
     * during it's execution. upper frame skips logging for uncaught exception only if
     * {@link #process} throws {@link InterruptedException} during shutdown
     * sequence.
     */
    void process(ProcessingContext<T> context, T task) throws InterruptedException;

    /**
     * @return A human-readable name for this processor.
     * This is used for observability (in particular, Zipkin traces), so it should be distinct enough
     * to let a reader identify how this processor is configured
     * (but does not necessarily have to be completely unique).
     */
    default String name() {
        return getClass().getSimpleName();
    }

    /**
     * The default close method which doesn't do anything.
     */
    @Override
    default void close() throws Exception {
        // noop
    }
}
