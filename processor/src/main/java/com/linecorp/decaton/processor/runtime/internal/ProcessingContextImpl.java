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

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.apache.kafka.common.header.Headers;

import com.linecorp.decaton.processor.DecatonProcessor;
import com.linecorp.decaton.processor.LoggingContext;
import com.linecorp.decaton.processor.ProcessingContext;
import com.linecorp.decaton.processor.TaskMetadata;
import com.linecorp.decaton.processor.Completion;
import com.linecorp.decaton.processor.Completion.TimeoutChoice;
import com.linecorp.decaton.processor.runtime.DecatonTask;
import com.linecorp.decaton.processor.runtime.ProcessorProperties;
import com.linecorp.decaton.processor.tracing.TracingProvider.ProcessorTraceHandle;
import com.linecorp.decaton.processor.tracing.TracingProvider.RecordTraceHandle;
import com.linecorp.decaton.processor.tracing.internal.NoopTracingProvider.NoopTrace;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ProcessingContextImpl<T> implements ProcessingContext<T> {
    private final String subscriptionId;
    private final TaskRequest request;
    private final DecatonTask<T> task;
    private final List<DecatonProcessor<T>> downstreams;
    private final DecatonProcessor<byte[]> retryQueueingProcessor;
    private final ProcessorProperties props;
    private final AtomicReference<CompletionImpl> deferredCompletion;

    public ProcessingContextImpl(String subscriptionId,
                                 TaskRequest request,
                                 DecatonTask<T> task,
                                 List<DecatonProcessor<T>> downstreams,
                                 DecatonProcessor<byte[]> retryQueueingProcessor,
                                 ProcessorProperties props) {
        this.subscriptionId = subscriptionId;
        this.request = request;
        this.task = task;
        this.downstreams = Collections.unmodifiableList(downstreams);
        this.retryQueueingProcessor = retryQueueingProcessor;
        this.props = props;
        deferredCompletion = new AtomicReference<>();
    }

    @Override
    public TaskMetadata metadata() {
        return task.metadata();
    }

    @Override
    public byte[] key() {
        return request.key();
    }

    @Override
    public Headers headers() {
        return request.headers();
    }

    @Override
    public LoggingContext loggingContext() {
        boolean enabled = props.get(ProcessorProperties.CONFIG_LOGGING_MDC_ENABLED).value();
        return new LoggingContext(enabled, subscriptionId, request, task.metadata());
    }

    @Override
    public String subscriptionId() {
        return subscriptionId;
    }

    @Override
    public Completion deferCompletion(Function<Completion, TimeoutChoice> callback) {
        if (deferredCompletion.get() == null) {
            CompletionImpl completion = new CompletionImpl();
            completion.expireCallback(callback);
            deferredCompletion.compareAndSet(null, completion);
        }
        return deferredCompletion.get();
    }

    private <P> Completion pushDownStream(List<DecatonProcessor<P>> downstreams, P taskData)
            throws InterruptedException {
        if (downstreams.isEmpty()) {
            // If there's no downstream associated with this processor, just drop the pushed task.
            return CompletionImpl.completedCompletion();
        }

        DecatonTask<P> task = new DecatonTask<>(
                this.task.metadata(), taskData, this.task.taskDataBytes());
        DecatonProcessor<P> nextProcessor = downstreams.get(0);
        final RecordTraceHandle parentTrace = request.trace();
        final ProcessorTraceHandle traceHandle = Utils.loggingExceptions(
                () -> parentTrace.childFor(nextProcessor),
                "Exception from tracing", NoopTrace.INSTANCE);
        ProcessingContextImpl<P> nextContext = new ProcessingContextImpl<>(
                subscriptionId, request, task, downstreams.subList(1, downstreams.size()),
                retryQueueingProcessor, props);

        CompletionImpl completion;
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
            completion = nextContext.deferredCompletion.get();
            if (completion == null) {
                // If process didn't requested for deferred completion, we understand it as process
                // completed synchronously.
                completion = CompletionImpl.completedCompletion();
            }
        }

        completion.asFuture().whenComplete((unused, throwable) -> {
            try {
                traceHandle.processingCompletion();
            } catch (Exception e) {
                log.error("Exception from tracing", e);
            }
        });
        return completion;
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
    public synchronized Completion push(T task) throws InterruptedException {
        return pushDownStream(downstreams, task);
    }

    @Override
    public Completion retry() throws InterruptedException {
        if (retryQueueingProcessor == null) {
            throw new IllegalStateException("task retry isn't configured for this processor");
        }

        Completion comp = deferCompletion();
        Completion retryCompletion = pushDownStream(Collections.singletonList(retryQueueingProcessor),
                                                    task.taskDataBytes());
        comp.completeWith(retryCompletion);
        return retryCompletion;
    }
}
