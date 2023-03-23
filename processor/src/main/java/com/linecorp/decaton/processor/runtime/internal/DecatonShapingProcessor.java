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

import java.util.concurrent.CompletableFuture;

import com.google.protobuf.ByteString;

import com.linecorp.decaton.client.PutTaskResult;
import com.linecorp.decaton.client.internal.DecatonTaskProducer;
import com.linecorp.decaton.processor.DecatonProcessor;
import com.linecorp.decaton.processor.ProcessingContext;
import com.linecorp.decaton.processor.metrics.Metrics;
import com.linecorp.decaton.processor.metrics.Metrics.ShapingMetrics;
import com.linecorp.decaton.processor.runtime.PerKeyQuotaConfig.QuotaCallback;
import com.linecorp.decaton.processor.runtime.PerKeyQuotaConfig.QuotaCallback.Action;
import com.linecorp.decaton.processor.runtime.internal.PerKeyQuotaManager.UsageType;
import com.linecorp.decaton.protocol.Decaton.DecatonTaskRequest;

public class DecatonShapingProcessor<T> implements DecatonProcessor<QuotaAwareTask<T>> {
    private final DecatonTaskProducer producer;
    private final QuotaCallback<T> quotaCallback;
    private final ShapingMetrics metrics;

    public DecatonShapingProcessor(SubscriptionScope scope,
                                   DecatonTaskProducer producer,
                                   QuotaCallback<T> quotaCallback) {
        this.producer = producer;
        this.quotaCallback = quotaCallback;
        metrics = Metrics.withTags("subscription", scope.subscriptionId()).new ShapingMetrics();
    }

    @Override
    public void process(ProcessingContext<QuotaAwareTask<T>> context, QuotaAwareTask<T> task)
            throws InterruptedException {
        if (task.quotaUsage().type() == UsageType.Violate) {
            Action action = quotaCallback.apply(context.metadata(),
                                                context.key(),
                                                task.task(),
                                                task.quotaUsage().metric());
            DecatonTaskRequest request =
                    DecatonTaskRequest.newBuilder()
                                      .setMetadata(context.metadata().toProto())
                                      .setSerializedTask(ByteString.copyFrom(task.taskDataBytes()))
                                      .build();
            CompletableFuture<PutTaskResult> future = producer.sendRequest(
                    action.topic(), context.key(), request, null);
            future.whenComplete((r, e) -> {
                if (e == null) {
                    metrics.shapingQueuedTasks.increment();
                } else {
                    metrics.shapingQueueingFailed.increment();
                }
            });
            context.deferCompletion().completeWith(future);
        } else {
            context.deferCompletion().completeWith(context.push(task));
        }
    }

    @Override
    public void close() throws Exception {
        producer.close();
        quotaCallback.close();
    }
}
