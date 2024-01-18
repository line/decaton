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

package com.linecorp.decaton.testing.processor;

import com.linecorp.decaton.processor.DecatonProcessor;

import lombok.ToString;
import lombok.Value;
import lombok.experimental.Accessors;

/**
 * A class which holds processed task along with processing time
 */
@Value
@Accessors(fluent = true)
public class ProcessedRecord {
    /**
     * Key of the task
     */
    @ToString.Exclude
    byte[] key;
    /**
     * Processed task
     */
    TestTask task;
    /**
     * {@link System#nanoTime()} at the time when started to process the task
     */
    long startTimeNanos;

    /**
     * {@link System#nanoTime()} at the time when {@link DecatonProcessor#process} for the task returned
     */
    long endTimeNanos;
}
