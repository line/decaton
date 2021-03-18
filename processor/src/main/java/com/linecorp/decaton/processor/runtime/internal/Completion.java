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

import com.linecorp.decaton.processor.DeferredCompletion;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;

@Accessors(fluent = true)
public class Completion implements DeferredCompletion {
    @RequiredArgsConstructor
    @Getter
    public static class CompletionExpire {
        private final long expireAt;
        private final Runnable callback;

        private Completion dependency;
    }

    @Getter
    private final CompletableFuture<Void> future;
    @Getter
    private volatile CompletionExpire expire;

    Completion() {
        future = new CompletableFuture<>();
    }

    @Override
    public void complete() {
        future.complete(null);
    }

    public boolean completed() {
        return future.isDone();
    }

    public void setTimeout(long timeoutAt, Runnable timeoutCallback) {
        expire = new CompletionExpire(timeoutAt, timeoutCallback);
    }

    public void timeoutWith(Completion comp) {
        expire.dependency = comp;
    }

    @Override
    public void completeWith(Completion comp) {
        timeoutWith(comp);
        comp.future().whenComplete((x, y) -> future.complete(null));
    }

    public boolean tryExpire() {
        if (expire == null) {
            return true;
        }

        if (expire.dependency != null) {
            if (!expire.dependency.tryExpire()) {
                return false;
            }
            expire.dependency = null;
        }

        long now = System.currentTimeMillis();
        if (expire.expireAt() <= now) {
            expire.callback().run();
            if (expire.expireAt() <= now) {
                complete();
                return true;
            }
        }
        return false;
    }
}
