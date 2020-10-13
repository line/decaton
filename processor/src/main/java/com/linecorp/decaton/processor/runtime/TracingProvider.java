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

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.linecorp.decaton.client.KafkaProducerSupplier;
import com.linecorp.decaton.processor.DecatonProcessor;
import com.linecorp.decaton.processor.DeferredCompletion;
import com.linecorp.decaton.processor.ProcessingContext;

/**
 * Interface for distributed tracing implementations that track traces via Kafka {@link ConsumerRecord}s
 * (typically through Kafka headers) e.g. Zipkin, OpenTracing, Kamon.
 * Note that this will usually need to be paired with a suitable {@link KafkaProducerSupplier} on the producer
 * side to write those Kafka headers.
 * In particular, if you want retries to be shown as part of the same (distributed) trace, make sure to supply
 * such a {@link KafkaProducerSupplier} in your {@link RetryConfig}.
 */
public interface TracingProvider {
    interface TraceHandle {
        /**
         * Invoked by the thread that will perform processing immediately before processing
         * Implementations may wish to associate the passed trace with the current thread
         * (e.g. using a {@link ThreadLocal}) for use during {@link DecatonProcessor} execution
         */
        void processingStart();

        /**
         * Invoked by the thread that performed processing immediately after *synchronous* processing finishes
         * (whether normally or exceptionally).
         * Implementations that associated this trace with the current thread in {@link #processingStart}
         * should dissociate it here.
         * Note that processing associated with this trace/record may continue (on other threads)
         * for some time after this call in the case of an asynchronous processor.
         */
        void processingReturn();

        /**
         * Invoked when processing is considered complete (whether normally or exceptionally).
         * If all processors in the pipeline are synchronous
         * (i.e. {@link ProcessingContext#deferCompletion()} was never called),
         * this will be invoked by the thread that performed processing immediately after processingReturn.
         * Otherwise, this will be invoked by the thread that calls {@link DeferredCompletion#complete()}.
         */
        void processingCompletion();
    }

    /**
     * Invoked as soon as we poll the record from Kakfa
     * (generally not from the thread that will be responsible for processing this record)
     *
     * @return an implementation-specific value to use when tracing processing of the given record.
     * The returned {@link TraceHandle} must be thread-safe.
     */
    TraceHandle traceFor(ConsumerRecord<?, ?> record, String subscriptionId);
}
