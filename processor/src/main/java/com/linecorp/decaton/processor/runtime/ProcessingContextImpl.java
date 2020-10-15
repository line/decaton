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

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import com.linecorp.decaton.processor.DecatonProcessor;
import com.linecorp.decaton.processor.DecatonTask;
import com.linecorp.decaton.processor.DeferredCompletion;
import com.linecorp.decaton.processor.ProcessingContext;
import com.linecorp.decaton.processor.TaskMetadata;
import com.linecorp.decaton.processor.runtime.NoopTracingProvider.NoopTrace;
import com.linecorp.decaton.processor.runtime.TracingProvider.TraceHandle;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ProcessingContextImpl<T> implements ProcessingContext<T> {
    private final String subscriptionId;
    private final TaskRequest request;
    private final DecatonTask<T> task;
    private final DeferredCompletion completion;
    private final List<DecatonProcessor<T>> downstreams;
    private final DecatonProcessor<byte[]> retryQueueingProcessor;
    AtomicBoolean completionDeferred;

    public ProcessingContextImpl(String subscriptionId,
                                 TaskRequest request,
                                 DecatonTask<T> task,
                                 DeferredCompletion completion,
                                 List<DecatonProcessor<T>> downstreams,
                                 DecatonProcessor<byte[]> retryQueueingProcessor) {
        this.subscriptionId = subscriptionId;
        this.request = request;
        this.task = task;
        this.completion = completion;
        this.downstreams = Collections.unmodifiableList(downstreams);
        this.retryQueueingProcessor = retryQueueingProcessor;
        completionDeferred = new AtomicBoolean();
    }

    public ProcessingContextImpl(String subscriptionId, TaskRequest request, DecatonTask<T> task,
                                 List<DecatonProcessor<T>> downstreams,
                                 DecatonProcessor<byte[]> retryQueueingProcessor) {
        this(subscriptionId, request, task, null, downstreams, retryQueueingProcessor);
    }

    @Override
    public TaskMetadata metadata() {
        return task.metadata();
    }

    @Override
    public String key() {
        return request.key();
    }

    @Override
    public LoggingContext loggingContext() {
        return new LoggingContext(subscriptionId, request, task.metadata());
    }

    @Override
    public String subscriptionId() {
        return subscriptionId;
    }

    @Override
    public DeferredCompletion deferCompletion() {
        completionDeferred.set(true);
        return completion;
    }

    private <P> CompletableFuture<Void> pushDownStream(List<DecatonProcessor<P>> downstreams, P taskData)
            throws InterruptedException {
        if (downstreams.isEmpty()) {
            // If there's no downstream associated with this processor, just drop the pushed task.
            return CompletableFuture.completedFuture(null);
        }

        DecatonTask<P> task = new DecatonTask<>(
                this.task.metadata(), taskData, this.task.taskDataBytes());
        DecatonProcessor<P> nextProcessor = downstreams.get(0);
        final TraceHandle parentTrace = request.trace();
        final TraceHandle traceHandle = null == parentTrace ? NoopTrace.INSTANCE
                                                            : parentTrace.childFor(nextProcessor);
        CompletableFuture<Void> future = new CompletableFuture<>();
        DeferredCompletion nextCompletion = () -> {
            future.complete(null);
            try {
                traceHandle.processingCompletion();
            } catch (Exception e) {
                log.error("Exception from tracing", e);
            }
        };
        ProcessingContextImpl<P> nextContext = new ProcessingContextImpl<>(
                subscriptionId, request, task, nextCompletion,
                downstreams.subList(1, downstreams.size()), retryQueueingProcessor);

        try {
            try {
                traceHandle.processingStart();
            } catch (Exception e) {
                log.error("Exception from tracing", e);
            }
            nextProcessor.process(nextContext, taskData);
            try {
                traceHandle.processingReturn();
            } catch (Exception e) {
                log.error("Exception from tracing", e);
            }
        } finally {
            if (!nextContext.completionDeferred.get()) {
                // If process didn't requested for deferred completion, we understand it as process
                // completed synchronously.
                nextCompletion.complete();
            }
        }

        return future;
    }

    /**
     * This method must be synchronized, as it can call downstream's
     * {@link DecatonProcessor#process} directly but upstream might call this method from a thread other than
     * {@link PartitionProcessor}'s internal threads.
     * In such case, since we don't know if the downstream processor is implemented taking account
     * thread-safety, we have to guarantee that the only one invocation of
     * {@link DecatonProcessor#process} occurs at the time from this context.
     */
    @Override
    public synchronized CompletableFuture<Void> push(T task) throws InterruptedException {
        return pushDownStream(downstreams, task);
    }

    @Override
    public CompletableFuture<Void> retry() throws InterruptedException {
        if (retryQueueingProcessor == null) {
            throw new IllegalStateException("task retry isn't configured for this processor");
        }

        DeferredCompletion completion = deferCompletion();
        CompletableFuture<Void> result = pushDownStream(
                Collections.singletonList(retryQueueingProcessor), task.taskDataBytes());
        return completion.completeWith(result);
    }
}
