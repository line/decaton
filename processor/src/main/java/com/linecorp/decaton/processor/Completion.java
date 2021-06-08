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

import java.util.concurrent.CompletionStage;

/**
 * An interface for completion status management of tasks.
 */
public interface Completion extends DeferredCompletion {
    /**
     * Choices on completion timeout.
     */
    enum TimeoutChoice {
        /**
         * Give up to complete this completion normally and forcefully complete it.
         */
        GIVE_UP,
        /**
         * Extend associated timeout and get back later.
         */
        EXTEND,
    }

    /**
     * Returns whether this completion is complete.
     * @return true if completed, false otherwise.
     */
    boolean isComplete();

    /**
     * Try to expire this completion. This method is intended for the use of decaton's internal implementation.
     * @return true if successfully expired, false otherwise.
     */
    boolean tryExpire();

    /**
     * Returns a {@link CompletionStage} that reflects completion state of this completion.
     * @return a {@link CompletionStage} linked with this completion.
     */
    CompletionStage<Void> asFuture();
}
