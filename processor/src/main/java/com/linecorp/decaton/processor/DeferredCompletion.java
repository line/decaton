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

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public interface DeferredCompletion {
    /**
     * Complete this deferred processing to tell it's ready for committing the corresponding offset.
     */
    void complete();

    /**
     * A helper method to let processor specify a subject {@link CompletableFuture} that matters to complete
     * this completion.
     * @param future a {@link CompletableFuture} which completes this {@link DeferredCompletion} along with it's
     * completion.
     * @return a {@link CompletableFuture} that completes after this completion completes.
     */
    default <T> CompletableFuture<T> completeWith(CompletableFuture<T> future) {
        return future.whenComplete((r, e) -> complete());
    }

    /**
     * Associate completion of this completion to the given {@link Completion}.
     * By calling this method for the completion X with the argument of A,
     * * when A completes, X completes
     * * when X times out, A times out
     *
     * Always prefer to use this method instead of {@link #completeWith(CompletableFuture)} when possible,
     * because associating a {@link Completion} to another {@link Completion} means not only to link their
     * completion but also their expiration with timeout. See
     * {@link ProcessingContext#deferCompletion(Function)} for the detail.
     *
     * @param dep a {@link Completion} to associate the completion.
     */
    void completeWith(Completion dep);
}
