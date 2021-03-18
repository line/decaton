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

import lombok.Getter;
import lombok.experimental.Accessors;

@Accessors(fluent = true)
public class OffsetState {
    @Getter
    private final long offset;
    @Getter
    private final Completion completion;

    OffsetState(long offset) {
        this.offset = offset;
        completion = new Completion();
    }

    public boolean completed() {
        return completion.completed();
    }

    @Override
    public String toString() {
        return "OffsetState{" +
               "offset=" + offset +
               ", committed=" + completed() +
               '}';
    }
}
