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

import java.util.function.Supplier;

import org.apache.kafka.clients.producer.KafkaProducer;

import com.linecorp.decaton.processor.DecatonProcessor;
import com.linecorp.decaton.processor.TaskMetadata;
import com.linecorp.decaton.processor.runtime.ProcessorSubscription;

/**
 * Interface for processing guarantee in Decaton stream-processing.
 *
 * Implementation classes should store produced records and processed records
 * when {@link #onProduce(ProducedRecord)} and {@link #onProcess(TaskMetadata, ProcessedRecord)} are invoked respectively,
 * and checks if the guarantee is met in {@link #doAssert()}
 */
public interface ProcessingGuarantee {
    /**
     * Guarantees that Decaton provides by default
     */
    enum GuaranteeType implements Supplier<ProcessingGuarantee> {
        /**
         * All produced messages are delivered and processed at least once.
         */
        AT_LEAST_ONCE_DELIVERY,
        /**
         * All messages are processed in order as produced per key, while allowing reprocessing.
         *
         * Note that if reprocessing happens, earlier tasks can be re-processed again
         * after later records got processed.
         *
         * Reprocessing can happen if a task is processed once but assigned to another instance by rebalance
         * before commiting offset and gets processed by another instance again.
         * This is likely to happen especially in some circumstances e.g. if you set max.pending.records to large value since
         * Decaton doesn't wait all pending tasks till complete before rebalance (and it's a design choice)
         *
         * Hence, what Decaton guarantees for process ordering can be formulated as below:
         * - if a task (offset:N) gets (re)processed, offset:N+M (M ≥ 1) always gets processed
         *
         * In addition, it's also be checked that if a task is committed, the task and preceding tasks never be reprocessed.
         *
         * Example:
         * <pre>
         * {@code
         *   say produced offsets are: [1,2,3,4,5]
         *   valid process ordering:
         *     - [1,2,3,4,5]
         *     - [1,2,1,2,3,4,5]: when processor restarted right after processed 2 before committing offset 1
         *     - [1,2,3,4,5,5]: when processor restarted right after processed 5 and committed 4 before committing offset 5
         *   invalid process ordering:
         *     - [1,3,2,4,5] => gapping
         *     - [1,2,3,4,5,4,3] => regressing to 4 implies offset 3 is committed. 3 cannot appear after processing 4
         * }
         * </pre>
         */
        PROCESS_ORDERING,
        /**
         * Tasks which have same key never be processed simultaneously
         */
        SERIAL_PROCESSING;

        @Override
        public ProcessingGuarantee get() {
            switch (this) {
                case AT_LEAST_ONCE_DELIVERY:
                    return new AtLeastOnceDelivery();
                case PROCESS_ORDERING:
                    return new ProcessOrdering();
                case SERIAL_PROCESSING:
                    return new SerialProcessing();
            }
            throw new RuntimeException("Unreachable");
        }
    }

    /**
     * Called when a task is produced.
     *
     * Must be thread safe since this method will be called from {@link KafkaProducer}'s I/O thread
     */
    void onProduce(ProducedRecord record);

    /**
     * Called when {@link DecatonProcessor#process} returned from processing a task.
     *
     * Must be thread safe since this method will be called from
     * multiple threads of multiple {@link ProcessorSubscription}
     */
    void onProcess(TaskMetadata metadata, ProcessedRecord record);

    /**
     * Checks if the processing guarantee is satisfied.
     *
     * Should throw {@link AssertionError} if the guarantee is not met
     */
    void doAssert();
}
