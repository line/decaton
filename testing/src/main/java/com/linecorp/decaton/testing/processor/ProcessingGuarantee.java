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

import java.util.EnumSet;
import java.util.function.Supplier;

import org.apache.kafka.clients.producer.KafkaProducer;

import com.linecorp.decaton.processor.runtime.ProcessorSubscription;

/**
 * Interface for processing guarantee in Decaton stream-processing.
 *
 * Implementation classes should store produced records and processed records
 * when {@link #onProduce(ProducedRecord)} and {@link #onProcess(ProcessedRecord)} are invoked respectively,
 * and checks if the guarantee is met in {@link #doAssert()}
 */
public interface ProcessingGuarantee {
    /**
     * Default processing semantics Decaton provides
     */
    static EnumSet<GuaranteeType> defaultSemantics() {
        return EnumSet.of(
                GuaranteeType.AT_LEAST_ONCE_DELIVERY,
                GuaranteeType.REPROCESS_AWARE_ORDERING,
                GuaranteeType.SERIAL_PROCESSING);
    }

    enum GuaranteeType implements Supplier<ProcessingGuarantee> {
        /**
         * All produced messages are delivered and processed at least once.
         */
        AT_LEAST_ONCE_DELIVERY,
        /**
         * All messages are processed in same order as produced, allowing re-processing.
         *
         * Example:
         * <pre>
         * {@code
         *   say produced offsets are: [1,2,3,4,5]
         *   valid processing order:
         *     - [1,2,3,4,5]
         *     - [1,2,1,2,3,4,5]: when processor restarted right after processed 2 before committing offset 1
         *     - [1,2,3,4,5,5]: when processor restarted right after processed 5 and committed 4 before committing offset 5
         *   invalid processing order:
         *     - [1,3,2,4,5] => gapping
         *     - [1,2,3,4,5,4,3] => re-consuming from 4 implies offset 3 is committed. 3 cannot appear after processing 4
         * }
         * </pre>
         */
        REPROCESS_AWARE_ORDERING,
        /**
         * Tasks which have same key never be processed simultaneously
         */
        SERIAL_PROCESSING;

        @Override
        public ProcessingGuarantee get() {
            switch (this) {
                case AT_LEAST_ONCE_DELIVERY:
                    return new AtLeastOnceDelivery();
                case REPROCESS_AWARE_ORDERING:
                    return new ReprocessAwareOrdering();
                case SERIAL_PROCESSING:
                    return new SerialProcessing();
            }
            throw new RuntimeException("Unreachable");
        }
    }

    /**
     * Called when a task is produced.
     *
     * Implementation must be concurrent aware since this method will be called from
     * {@link KafkaProducer}'s I/O thread
     */
    void onProduce(ProducedRecord record);

    /**
     * Called when a task has been completed to process.
     *
     * Implementation must be concurrent aware since this method will be called from
     * multiple threads of multiple {@link ProcessorSubscription}
     */
    void onProcess(ProcessedRecord record);

    /**
     * Checks if the processing guarantee is satisfied.
     *
     * Implementation classes should throw {@link AssertionError} if the guarantee is not met
     */
    void doAssert();
}
