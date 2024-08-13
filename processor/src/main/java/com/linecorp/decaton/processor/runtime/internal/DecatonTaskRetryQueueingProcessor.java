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

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.producer.ProducerRecord;

import com.google.protobuf.ByteString;

import com.linecorp.decaton.client.internal.DecatonTaskProducer;
import com.linecorp.decaton.client.PutTaskResult;
import com.linecorp.decaton.client.internal.TaskMetadataUtil;
import com.linecorp.decaton.processor.DecatonProcessor;
import com.linecorp.decaton.processor.ProcessingContext;
import com.linecorp.decaton.processor.TaskMetadata;
import com.linecorp.decaton.processor.metrics.Metrics;
import com.linecorp.decaton.processor.metrics.Metrics.RetryMetrics;
import com.linecorp.decaton.processor.runtime.ProcessorProperties;
import com.linecorp.decaton.processor.runtime.Property;
import com.linecorp.decaton.processor.runtime.RetryConfig;
import com.linecorp.decaton.protocol.internal.DecatonInternal.DecatonTaskRequest;
import com.linecorp.decaton.protocol.Decaton.TaskMetadataProto;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DecatonTaskRetryQueueingProcessor implements DecatonProcessor<byte[]> {
    private final DecatonTaskProducer producer;
    private final Duration backoff;
    private final RetryMetrics metrics;
    private final String retryTopic;
    private final Property<Boolean> metadataAsHeaderProperty;

    public DecatonTaskRetryQueueingProcessor(SubscriptionScope scope, DecatonTaskProducer producer) {
        RetryConfig retryConfig = scope.retryConfig().get(); // This won't be instantiated unless it present
        this.producer = producer;
        backoff = retryConfig.backoff();
        retryTopic = scope.retryTopic().get(); // This won't be instantiated unless it present
        metadataAsHeaderProperty = scope.props().get(ProcessorProperties.CONFIG_TASK_METADATA_AS_HEADER);

        metrics = Metrics.withTags("subscription", scope.subscriptionId()).new RetryMetrics();
    }

    @Override
    public void process(ProcessingContext<byte[]> context, byte[] serializedTask)
            throws InterruptedException {
        TaskMetadata originalMeta = context.metadata();
        long nextRetryCount = originalMeta.retryCount() + 1;
        long nextTryTimeMillis = System.currentTimeMillis() + backoff.toMillis();
        TaskMetadataProto taskMetadata =
                TaskMetadataProto.newBuilder(originalMeta.toProto())
                                 .setRetryCount(nextRetryCount)
                                 .setScheduledTimeMillis(nextTryTimeMillis)
                                 .build();

        final ProducerRecord<byte[], byte[]> record;
        if (metadataAsHeaderProperty.value()) {
            record = new ProducerRecord<>(
                    retryTopic,
                    null,
                    context.key(),
                    serializedTask,
                    context.headers());
            TaskMetadataUtil.writeAsHeader(taskMetadata, record.headers());
        } else {
            DecatonTaskRequest request =
                    DecatonTaskRequest.newBuilder()
                                      .setMetadata(taskMetadata)
                                      .setSerializedTask(ByteString.copyFrom(serializedTask))
                                      .build();
            record = new ProducerRecord<>(
                    retryTopic,
                    null,
                    context.key(),
                    request.toByteArray(),
                    context.headers());
        }
        metrics.retryTaskRetries.record(nextRetryCount);

        CompletableFuture<PutTaskResult> future = producer.sendRequest(record);
        future.whenComplete((r, e) -> {
            if (e == null) {
                metrics.retryQueuedTasks.increment();
            } else {
                metrics.retryQueueingFailed.increment();
            }
        });
        context.deferCompletion().completeWith(future);
    }

    @Override
    public void close() throws Exception {
        producer.close();
        metrics.close();
    }
}
