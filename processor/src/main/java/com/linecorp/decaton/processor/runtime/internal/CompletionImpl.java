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

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import com.linecorp.decaton.processor.Completion;

import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Accessors(fluent = true)
public class CompletionImpl implements Completion {
    private final CompletableFuture<Void> future;
    @Setter
    private volatile Function<Completion, TimeoutChoice> expireCallback;
    private volatile Completion dependency;

    public static CompletionImpl completedCompletion() {
        CompletionImpl comp = new CompletionImpl();
        comp.complete();
        return comp;
    }

    static CompletionImpl failedCompletion(Throwable ex) {
        CompletionImpl comp = new CompletionImpl();
        comp.future.completeExceptionally(ex);
        return comp;
    }

    public CompletionImpl() {
        future = new CompletableFuture<>();
    }

    @Override
    public boolean isComplete() {
        return future.isDone();
    }

    @Override
    public CompletionStage<Void> asFuture() {
        return future;
    }

    @Override
    public boolean tryExpire() {
        if (isComplete()) {
            return true;
        }
        if (dependency != null) {
            if (!dependency.tryExpire()) {
                return false;
            }
            dependency = null;
        }
        Function<Completion, TimeoutChoice> cb = expireCallback;
        if (cb == null) {
            return true;
        }
        try {
            return cb.apply(this) == TimeoutChoice.GIVE_UP;
        } catch (RuntimeException e) {
            log.warn("Completion timeout callback threw an exception", e);
            return false;
        }
    }

    @Override
    public void completeWith(Completion dep) {
        dependency = Objects.requireNonNull(dep, "dep");
        // Assigning to local variable once in order to let the following closure capture reference for CF
        // itself rather than for this CompletionImpl object.
        // https://github.com/kawamuray/notes/blob/master/java.adoc#field-reference-from-closure-and-gc
        CompletableFuture<Void> futLocal = future;
        dep.asFuture().whenComplete((unused, throwable) -> futLocal.complete(null));
    }

    @Override
    public void complete() {
        future.complete(null);
    }
}
