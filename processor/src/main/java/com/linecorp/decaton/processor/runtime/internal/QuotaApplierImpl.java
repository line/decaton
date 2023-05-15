/*
 * Copyright 2023 LINE Corporation
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

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.linecorp.decaton.processor.Completion;
import com.linecorp.decaton.processor.metrics.Metrics;
import com.linecorp.decaton.processor.metrics.Metrics.ShapingMetrics;
import com.linecorp.decaton.processor.runtime.PerKeyQuotaConfig.QuotaCallback;
import com.linecorp.decaton.processor.runtime.internal.PerKeyQuotaManager.QuotaUsage;
import com.linecorp.decaton.processor.runtime.internal.PerKeyQuotaManager.UsageType;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class QuotaApplierImpl implements QuotaApplier {
    // visible for testing
    final ExecutorService shapingExecutor;
    private final Producer<byte[], byte[]> producer;
    private final QuotaCallback callback;
    private final ShapingMetrics metrics;
    private final AtomicBoolean terminated = new AtomicBoolean(false);

    public QuotaApplierImpl(Producer<byte[], byte[]> producer,
                QuotaCallback callback,
                SubscriptionScope scope) {
        this.producer = producer;
        this.callback = callback;
        shapingExecutor = Executors.newSingleThreadExecutor(Utils.namedThreadFactory("TaskShaper"));
        metrics = Metrics.withTags("subscription", scope.subscriptionId()).new ShapingMetrics();
    }

    @Override
    public boolean apply(ConsumerRecord<byte[], byte[]> record,
                         OffsetState offsetState,
                         QuotaUsage quotaUsage) {
        if (quotaUsage == null || quotaUsage.type() == UsageType.COMPLY) {
            return false;
        }

        Completion completion = offsetState.completion();
        shapingExecutor.execute(() -> {
            if (terminated.get()) {
                return;
            }
            final String topic;
            try {
                topic = callback.apply(record, quotaUsage.metrics());
            } catch (Exception e) {
                log.error("Exception thrown from the quota callback for key: {}", Arrays.toString(record.key()), e);
                metrics.shapingQueueingFailed.increment();
                completion.complete();
                return;
            }

            ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(
                    topic, null, record.key(), record.value(), record.headers());
            if (terminated.get()) {
                return;
            }
            try {
                producer.send(producerRecord, (r, e) -> {
                    // Here, processor subscription is about to be closed
                    // (i.e. no more offsets will be committed), so we don't care the send result.
                    // Regardless send is succeeded or not, the task will be processed again by other instances after the rebalance.
                    if (terminated.get()) {
                        return;
                    }

                    if (e == null) {
                        metrics.shapingQueuedTasks.increment();
                    } else {
                        metrics.shapingQueueingFailed.increment();
                        log.error("Failed to send task to the shaping topic", e);
                    }
                    completion.complete();
                });
            } catch (IllegalStateException e) {
                // This exception is thrown when send() is called after the producer is closed.
                // This still can happen if terminated flag is enabled right after we checked before send() call.
                // This is an expected behavior, so we just ignore the exception.
                log.debug("Failed to send task to the shaping topic upon close", e);
            } catch (Exception e) {
                log.error("Exception thrown while sending task to the shaping topic", e);
                metrics.shapingQueueingFailed.increment();
                completion.complete();
            }
        });

        return true;
    }

    @Override
    public void close() {
        terminated.set(true);
        // As the subscription is about to be closed, there's no point to wait for the tasks to be produced.
        // Closing the producer with zero timeout will fail all queued batches.
        safeClose(() -> producer.close(Duration.ZERO), "shaping producer");

        shapingExecutor.shutdown();
        try {
            shapingExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while awaiting shaping tasks are produced", e);
            // We don't re-throw the exception here to clean-up other resources
        }

        safeClose(callback::close, "quota callback");
    }

    private static void safeClose(Runnable closer, String name) {
        try {
            closer.run();
        } catch (Exception e) {
            log.error("Exception thrown while closing {}", name, e);
        }
    }
}
