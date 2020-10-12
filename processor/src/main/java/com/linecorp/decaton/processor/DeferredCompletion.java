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

@FunctionalInterface
public interface DeferredCompletion {
    /**
     * Complete this deferred processing to tell it's ready for committing corresponding offset.
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

    static DeferredCompletion combine(DeferredCompletion... completions) {
        return () -> {
            for (DeferredCompletion completion : completions) {
                completion.complete();
            }
        };
    }
}
