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

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.linecorp.decaton.processor.Completion;
import com.linecorp.decaton.processor.metrics.Metrics;
import com.linecorp.decaton.processor.metrics.Metrics.ShapingMetrics;
import com.linecorp.decaton.processor.runtime.PerKeyQuotaConfig.QuotaCallback;
import com.linecorp.decaton.processor.runtime.PerKeyQuotaConfig.QuotaCallback.Action;
import com.linecorp.decaton.processor.runtime.internal.PerKeyQuotaManager.UsageType;

import lombok.extern.slf4j.Slf4j;

/**
 * An interface which is responsible for applying quota to a {@link TaskRequest}.
 * When per-key-quota feature is not enabled, you should use {@link QuotaApplier.NoopApplier}
 */
public interface QuotaApplier extends AutoCloseable {
    boolean apply(TaskRequest request);

    @Override
    void close();

    enum NoopApplier implements QuotaApplier {
        INSTANCE;

        @Override
        public boolean apply(TaskRequest request) {
            return false;
        }

        @Override
        public void close() {}
    }

    @Slf4j
    class Impl implements QuotaApplier {
        private final ExecutorService shapingExecutor;
        private final Producer<byte[], byte[]> producer;
        private final QuotaCallback callback;
        private final ShapingMetrics metrics;

        public Impl(Producer<byte[], byte[]> producer,
                    QuotaCallback callback,
                    SubscriptionScope scope) {
            this.producer = producer;
            this.callback = callback;
            shapingExecutor = Executors.newSingleThreadExecutor(Utils.namedThreadFactory("TaskShaper"));
            metrics = Metrics.withTags("subscription", scope.subscriptionId()).new ShapingMetrics();
        }

        /**
         * Maybe applying a quota to the given {@link TaskRequest}.
         * If quota is applied, this method will handle the task accordingly based on
         * the {@link QuotaCallback} result and return true.
         * Otherwise, this method will return false.
         * If true is returned, the caller should not enqueue the task to the processor.
         * @param request the task request to apply quota.
         * @return true if quota is applied, false otherwise.
         */
        @Override
        public boolean apply(TaskRequest request) {
            if (request.quotaUsage() == null || request.quotaUsage().type() == UsageType.COMPLY) {
                return false;
            }

            final Action action;
            Completion completion = request.offsetState().completion();
            try {
                action = callback.apply(request.key(), request.quotaUsage().metric());
            } catch (Exception e) {
                log.error("Exception thrown from the quota callback for key: {}", Arrays.toString(request.key()), e);
                metrics.shapingQueueingFailed.increment();
                completion.complete();
                return true;
            }

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
                    action.topic(), null, request.key(), request.rawRequestBytes(), request.headers());
            request.purgeRawRequestBytes();
            shapingExecutor.execute(() -> {
                try {
                    producer.send(record, (r, e) -> {
                        if (e == null) {
                            metrics.shapingQueuedTasks.increment();
                        } else {
                            metrics.shapingQueueingFailed.increment();
                            log.error("Failed to send task to the shaping topic", e);
                        }
                        completion.complete();
                    });
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
            shapingExecutor.shutdown();
            try {
                shapingExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Interrupted while awaiting shaping tasks are produced", e);
                // We don't re-throw the exception here to clean-up other resources
            }

            safeClose(producer, "shaping producer");
            safeClose(producer, "quota callback");
        }

        private static void safeClose(AutoCloseable closeable, String name) {
            try {
                closeable.close();
            } catch (Exception e) {
                log.error("Exception thrown while closing {}", name, e);
            }
        }
    }
}
