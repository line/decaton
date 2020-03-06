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

/**
 * Enumerates the possible "scope"s of processor's lifecycle.
 * Depending on which value to specify for given processor supplier, Decaton controls processor's creation
 * time and destruction time manage processor's lifecycle.
 */
public enum ProcessorScope {
    /**
     * The processor is supplied externally so Decaton just needs to use it but stay untouched for destruction.
     */
    PROVIDED,
    /**
     * The processor is singleton hence the instance is created only once in {@link ProcessorSubscription} and
     * reused for all partitions assigned to it.
     */
    SINGLETON,
    /**
     * A processor created one for each partition. When {@link ProcessorSubscription} lost assignment for a
     * partition instance gets destructed.
     */
    PARTITION,
    /**
     * A processor created one for each partition processor threads. When {@link ProcessorSubscription} lost
     * assignment for a partition all instances belonged to threads for the partition gets destructed.
     */
    THREAD,
    ;
}
