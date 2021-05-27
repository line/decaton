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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import com.linecorp.decaton.processor.runtime.Completion;

import lombok.Data;
import lombok.experimental.Accessors;

@Accessors(fluent = true)
@Data
public class CompletionImpl implements Completion {
    private final CompletableFuture<Void> future;
    private Supplier<Boolean> expireCallback;
    private Completion dependency;

    public static CompletionImpl completedCompletion() {
        CompletionImpl comp = new CompletionImpl();
        comp.complete();
        return comp;
    }

    public CompletionImpl() {
        future = new CompletableFuture<>();
    }

    @Override
    public boolean hasComplete() {
        return future.isDone();
    }

    @Override
    public CompletionStage<Void> asFuture() {
        return future;
    }

    @Override
    public boolean tryExpire() {
        if (dependency != null) {
            dependency.tryExpire();
            if (!dependency.hasComplete()) {
                return true;
            }
        }
        return expireCallback.get();
    }

    @Override
    public void completeWith(Completion dep) {
        dependency = dep;
        dep.asFuture().whenComplete((unused, throwable) -> future.complete(null));
    }

    @Override
    public void complete() {
        future.complete(null);
    }
}
