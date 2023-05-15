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

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.apache.kafka.common.TopicPartition;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import com.linecorp.decaton.processor.DecatonProcessor;
import com.linecorp.decaton.processor.DeferredCompletion;
import com.linecorp.decaton.processor.ProcessingContext;
import com.linecorp.decaton.processor.TaskMetadata;
import com.linecorp.decaton.processor.Completion;
import com.linecorp.decaton.processor.Completion.TimeoutChoice;
import com.linecorp.decaton.processor.runtime.DecatonTask;
import com.linecorp.decaton.processor.runtime.ProcessorProperties;
import com.linecorp.decaton.processor.tracing.TestTraceHandle;
import com.linecorp.decaton.processor.tracing.TestTracingProvider;
import com.linecorp.decaton.processor.tracing.TracingProvider.RecordTraceHandle;
import com.linecorp.decaton.processor.tracing.internal.NoopTracingProvider.NoopTrace;
import com.linecorp.decaton.protocol.Decaton.DecatonTaskRequest;
import com.linecorp.decaton.protocol.Decaton.TaskMetadataProto;
import com.linecorp.decaton.protocol.Sample.HelloTask;

public class ProcessingContextImplTest {
    private static class NamedProcessor implements DecatonProcessor<HelloTask> {
        private final String name;
        private final DecatonProcessor<HelloTask> impl;

        private NamedProcessor(String name, DecatonProcessor<HelloTask> impl) {
            this.name = name;
            this.impl = impl;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public void process(ProcessingContext<HelloTask> ctx, HelloTask task)
                throws InterruptedException {
            impl.process(ctx, task);
        }
    }

    private static final HelloTask TASK = HelloTask.getDefaultInstance();

    private static final DecatonTaskRequest REQUEST =
            DecatonTaskRequest.newBuilder()
                              .setMetadata(TaskMetadataProto.getDefaultInstance())
                              .setSerializedTask(TASK.toByteString())
                              .build();

    @Rule
    public MockitoRule rule = MockitoJUnit.rule();

    @Mock
    private NamedProcessor processorMock;

    private static void terminateExecutor(ExecutorService executor) throws InterruptedException {
        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
    }

    @SafeVarargs
    private static ProcessingContextImpl<HelloTask> context(RecordTraceHandle traceHandle,
                                                            DecatonProcessor<HelloTask>... processors) {
        TaskRequest request = new TaskRequest(
                new TopicPartition("topic", 1), 1, null, "TEST".getBytes(StandardCharsets.UTF_8),
                null, traceHandle, REQUEST.toByteArray(), null);
        DecatonTask<HelloTask> task = new DecatonTask<>(
                TaskMetadata.fromProto(REQUEST.getMetadata()), TASK, TASK.toByteArray());
        return new ProcessingContextImpl<>("subscription", request, task, Arrays.asList(processors),
                                           null, ProcessorProperties.builder().build());
    }

    private static void safeAwait(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static ExecutorService[] executors(int size) {
        ExecutorService[] executors = new ExecutorService[size];
        for (int i = 0; i < size; i++) {
            executors[i] = Executors.newSingleThreadExecutor();
        }
        return executors;
    }

    private static CountDownLatch[] latches(int size) {
        CountDownLatch[] latches = new CountDownLatch[size];
        for (int i = 0; i < size; i++) {
            latches[i] = new CountDownLatch(1);
        }
        return latches;
    }

    @Test(timeout = 5000)
    public void testPush_Level1_Sync() throws InterruptedException {
        ProcessingContextImpl<HelloTask> context = context(NoopTrace.INSTANCE, (ctx, task) -> { /* noop */ });

        Completion comp = context.push(TASK);
        assertTrue(comp.isComplete());
    }

    @Test(timeout = 5000)
    public void testPush_Level1_Async() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();

        AtomicBoolean timeoutCbCalled = new AtomicBoolean();
        Function<Completion, TimeoutChoice> timeoutCb = comp -> {
            timeoutCbCalled.set(true);
            return TimeoutChoice.GIVE_UP;
        };
        ProcessingContextImpl<HelloTask> context = context(NoopTrace.INSTANCE, (ctx, task) -> {
            DeferredCompletion comp = ctx.deferCompletion(timeoutCb);
            executor.execute(() -> {
                safeAwait(latch);
                comp.complete();
            });
        });

        Completion comp = context.push(TASK);

        assertFalse(comp.isComplete());
        comp.tryExpire();
        assertTrue(timeoutCbCalled.get());

        latch.countDown();
        terminateExecutor(executor);
        assertTrue(comp.isComplete());
    }

    /**
     * This test is more a likely testing usage of {@link CompletableFuture} than a logic of
     * {@link ProcessingContextImpl} but still we want to keep it to make sure that the our processing model
     * works consistently to what we expect.
     */
    @Test(timeout = 5000)
    public void testPush_Level1_MultiPush_BothSync() throws InterruptedException {
        ProcessingContextImpl<HelloTask> context = context(NoopTrace.INSTANCE, (ctx, task) -> { /* noop */ });

        Completion comp1 = context.push(TASK);
        Completion comp2 = context.push(TASK);
        CompletableFuture<Void> fAll = CompletableFuture.allOf(comp1.asFuture().toCompletableFuture(),
                                                               comp2.asFuture().toCompletableFuture());

        assertTrue(comp1.isComplete());
        assertTrue(comp2.isComplete());
        assertTrue(fAll.isDone());
    }

    /**
     * This test is more a likely testing usage of {@link CompletableFuture} than a logic of
     * {@link ProcessingContextImpl} but still we want to keep it to make sure that the our processing model
     * works consistently to what we expect.
     */
    @Test(timeout = 5000)
    public void testPush_Level1_MultiPush_BothAsync() throws InterruptedException {
        CountDownLatch[] latches = latches(2);
        ExecutorService[] executors = executors(2);
        AtomicInteger processCount = new AtomicInteger();

        ProcessingContextImpl<HelloTask> context = context(NoopTrace.INSTANCE, (ctx, task) -> {
            DeferredCompletion comp = ctx.deferCompletion();
            int i = processCount.getAndIncrement();
            executors[i].execute(() -> {
                safeAwait(latches[i]);
                comp.complete();
            });
        });

        Completion comp1 = context.push(TASK);
        Completion comp2 = context.push(TASK);
        CompletableFuture<Void> fAll = CompletableFuture.allOf(comp1.asFuture().toCompletableFuture(),
                                                               comp2.asFuture().toCompletableFuture());

        assertFalse(fAll.isDone());
        latches[0].countDown();
        terminateExecutor(executors[0]);
        assertTrue(comp1.isComplete());
        assertFalse(fAll.isDone());
        latches[1].countDown();
        terminateExecutor(executors[1]);
        assertTrue(comp2.isComplete());
        assertTrue(fAll.isDone());
    }

    @Test(timeout = 5000)
    public void testPush_Level2_Sync() throws InterruptedException {
        ProcessingContextImpl<HelloTask> context = context(NoopTrace.INSTANCE, ProcessingContext::push,
                                                           processorMock);

        Completion comp = context.push(TASK);
        assertTrue(comp.isComplete());
        verify(processorMock, times(1)).process(any(), eq(TASK));
    }

    @Test(timeout = 5000)
    public void testPush_Level2_Sync_ThenAsync() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        doAnswer(invocation -> {
            ProcessingContext<HelloTask> ctx = invocation.getArgument(0);
            DeferredCompletion comp = ctx.deferCompletion();
            executor.execute(() -> {
                safeAwait(latch);
                comp.complete();
            });
            return null;
        }).when(processorMock).process(any(), eq(TASK));

        ProcessingContextImpl<HelloTask> context = context(NoopTrace.INSTANCE, ProcessingContext::push,
                                                           processorMock);

        Completion comp = context.push(TASK);
        assertTrue(comp.isComplete());
        verify(processorMock, times(1)).process(any(), eq(TASK));
    }

    @Test(timeout = 5000)
    public void testPush_Level2_Async_ThenAsync() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        doAnswer(invocation -> {
            ProcessingContext<HelloTask> ctx = invocation.getArgument(0);
            DeferredCompletion comp = ctx.deferCompletion();
            executor.execute(() -> {
                safeAwait(latch);
                comp.complete();
            });
            return null;
        }).when(processorMock).process(any(), eq(TASK));

        ProcessingContextImpl<HelloTask> context = context(NoopTrace.INSTANCE,
                                                           (ctx, task) -> ctx.deferCompletion()
                                                                             .completeWith(ctx.push(task)),
                                                           processorMock);

        Completion comp = context.push(TASK);
        assertFalse(comp.isComplete());
        verify(processorMock, times(1)).process(any(), eq(TASK));

        latch.countDown();
        terminateExecutor(executor);
        assertTrue(comp.isComplete());
    }

    /**
     * This test is more a likely testing usage of {@link CompletableFuture} than a logic of
     * {@link ProcessingContextImpl} but still we want to keep it to make sure that the our processing model
     * works consistently to what we expect.
     */
    @Test(timeout = 5000)
    public void testPush_Level2_MultiPush_SyncAndAsync() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        AtomicInteger processCount = new AtomicInteger();

        doAnswer(invocation -> {
            ProcessingContext<HelloTask> ctx = invocation.getArgument(0);

            int i = processCount.getAndIncrement();
            if (i == 0) {
                return null; // Downstream process 1 completes synchronously
            }
            // Downstream process 2 completes asynchronously
            DeferredCompletion comp = ctx.deferCompletion();
            executor.execute(() -> {
                safeAwait(latch);
                comp.complete();
            });
            return null;
        }).when(processorMock).process(any(), eq(TASK));

        ProcessingContextImpl<HelloTask> context = context(NoopTrace.INSTANCE,
                                                           (ctx, task) -> ctx.deferCompletion()
                                                                             .completeWith(ctx.push(TASK)),
                                                           processorMock);

        Completion comp1 = context.push(TASK);
        Completion comp2 = context.push(TASK);
        CompletableFuture<Void> fAll = CompletableFuture.allOf(comp1.asFuture().toCompletableFuture(),
                                                               comp2.asFuture().toCompletableFuture());

        assertTrue(comp1.isComplete());
        assertFalse(comp2.isComplete());
        assertFalse(fAll.isDone());

        latch.countDown();
        terminateExecutor(executor);
        assertTrue(comp1.isComplete());
        assertTrue(comp2.isComplete());
        assertTrue(fAll.isDone());
    }

    @Test(timeout = 5000)
    public void testRetry() throws InterruptedException {
        CountDownLatch retryLatch = new CountDownLatch(1);
        DecatonProcessor<byte[]> retryProcessor = spy(
                // This can't be a lambda for mockito
                new DecatonProcessor<byte[]>() {
                    @Override
                    public void process(ProcessingContext<byte[]> context, byte[] task)
                            throws InterruptedException {
                        Completion comp = context.deferCompletion();
                        new Thread(() -> {
                            try {
                                retryLatch.await();
                            } catch (InterruptedException ignored) {}
                            comp.complete();
                        }).start();
                    }
                });
        TaskRequest request = new TaskRequest(
                new TopicPartition("topic", 1), 1, null, "TEST".getBytes(StandardCharsets.UTF_8), null, null, REQUEST.toByteArray(), null);
        DecatonTask<byte[]> task = new DecatonTask<>(
                TaskMetadata.fromProto(REQUEST.getMetadata()), TASK.toByteArray(), TASK.toByteArray());

        ProcessingContextImpl<byte[]> context =
                spy(new ProcessingContextImpl<>("subscription", request, task,
                                                Collections.emptyList(), retryProcessor,
                                                ProcessorProperties.builder().build()));

        Completion retryComp = context.retry();

        // Check that the returned completion is associated with retry's completion
        verify(retryProcessor).process(any(), eq(TASK.toByteArray()));
        assertFalse(retryComp.isComplete());

        retryLatch.countDown();
        retryComp.asFuture().toCompletableFuture().join();
        assertTrue(retryComp.isComplete());
    }

    @Test(expected = IllegalStateException.class)
    public void testRetry_NOT_CONFIGURED() throws InterruptedException {
        context(NoopTrace.INSTANCE).retry();
    }

    @Test(timeout = 5000)
    public void testRetryAtCompletionTimeout() throws InterruptedException {
        CountDownLatch retryLatch = new CountDownLatch(1);
        DecatonProcessor<byte[]> retryProcessor = spy(
                // This can't be a lambda for mockito
                new DecatonProcessor<byte[]>() {
                    @Override
                    public void process(ProcessingContext<byte[]> context, byte[] task)
                            throws InterruptedException {
                        Completion comp = context.deferCompletion();
                        new Thread(() -> {
                            try {
                                retryLatch.await();
                            } catch (InterruptedException ignored) {}
                            comp.complete();
                        }).start();
                    }
                });
        TaskRequest request = new TaskRequest(
                new TopicPartition("topic", 1), 1, null, "TEST".getBytes(StandardCharsets.UTF_8), null, null, REQUEST.toByteArray(), null);
        DecatonTask<byte[]> task = new DecatonTask<>(
                TaskMetadata.fromProto(REQUEST.getMetadata()), TASK.toByteArray(), TASK.toByteArray());

        DecatonProcessor<byte[]> processor = (context, ignored) -> context.deferCompletion(comp -> {
            try {
                 comp.completeWith(context.retry());
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return TimeoutChoice.EXTEND;
        });

        ProcessingContextImpl<byte[]> context =
                spy(new ProcessingContextImpl<>("subscription", request, task,
                                                Arrays.asList(processor), retryProcessor,
                                                ProcessorProperties.builder().build()));

        Completion comp = context.push(new byte[0]);
        comp.tryExpire();
        // Check that the returned completion is associated with retry's completion
        verify(retryProcessor).process(any(), eq(TASK.toByteArray()));
        assertFalse(comp.isComplete());

        retryLatch.countDown();
        comp.asFuture().toCompletableFuture().join();

        assertTrue(comp.isComplete());
    }

    @Test(timeout = 5000)
    public void testTrace_Sync() throws InterruptedException {
        RecordTraceHandle handle = new TestTraceHandle("testTrace_Sync");
        final AtomicReference<String> traceDuringProcessing = new AtomicReference<>();
        try {
            ProcessingContextImpl<HelloTask> context = context(handle,
                                                               new NamedProcessor("Noop", (ctx, task) ->
                                                                       traceDuringProcessing
                                                                               .set(TestTracingProvider
                                                                                            .getCurrentTraceId())
                                                               ));
            Completion comp = context.push(TASK);
            assertTrue(comp.isComplete());
            assertNull(TestTracingProvider.getCurrentTraceId());
            assertEquals("testTrace_Sync-Noop", traceDuringProcessing.get());
        } finally {
            handle.processingCompletion();
        }
        TestTracingProvider.assertAllTracesWereClosed();
    }

    @Test(timeout = 5000)
    public void testTrace_Async() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        RecordTraceHandle handle = new TestTraceHandle("testTrace_Async");
        final AtomicReference<String> traceDuringSyncProcessing = new AtomicReference<>();
        final AtomicReference<String> traceDuringAsyncProcessing = new AtomicReference<>();
        try {
            ProcessingContextImpl<HelloTask> context =
                    context(handle, new NamedProcessor("Async", (ctx, task) -> {
                        DeferredCompletion comp = ctx.deferCompletion();
                        traceDuringSyncProcessing.set(TestTracingProvider.getCurrentTraceId());
                        executor.execute(() -> {
                            traceDuringAsyncProcessing.set(TestTracingProvider.getCurrentTraceId());
                            safeAwait(latch);
                            comp.complete();
                        });
                    }));

            Completion comp = context.push(TASK);
            assertFalse(comp.isComplete());
            // The trace for this processor should no longer be "current"
            // (because sync execution has finished)
            // but it is should still be "open"
            assertNull(TestTracingProvider.getCurrentTraceId());
            assertThat(TestTracingProvider.getOpenTraces(), hasItem("testTrace_Async-Async"));

            latch.countDown();
            terminateExecutor(executor);
            assertTrue(comp.isComplete());
            assertThat(TestTracingProvider.getOpenTraces(), not(hasItem("testTrace_Async-Async")));

            assertEquals("testTrace_Async-Async", traceDuringSyncProcessing.get());
            // Trace ID is not propagated unless the implementation does so manually
            assertNull(traceDuringAsyncProcessing.get());
        } finally {
            handle.processingCompletion();
        }
        TestTracingProvider.assertAllTracesWereClosed();
    }
}
