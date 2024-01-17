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
 * Configuration options to set subpartitions runtime mode.
 */
public enum SubPartitionRuntime {
    /**
     * Fixed number of subpartitions along with an associated physical thread for each to process
     * queues routed by the {@link SubPartitioner}.
     * This mode respects {@link ProcessorProperties#CONFIG_PARTITION_CONCURRENCY} to determine the
     * number of subpartitions for each partition.
     */
    THREAD_POOL,
    /**
     * Unbound amount of virtual threads, associated to each unique key will be created and used to
     * process each partition.
     */
    VIRTUAL_THREAD,
    ;
}
