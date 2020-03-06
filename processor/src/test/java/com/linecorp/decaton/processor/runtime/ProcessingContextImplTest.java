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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.common.TopicPartition;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import com.linecorp.decaton.processor.DecatonProcessor;
import com.linecorp.decaton.processor.DecatonTask;
import com.linecorp.decaton.processor.DeferredCompletion;
import com.linecorp.decaton.processor.ProcessingContext;
import com.linecorp.decaton.processor.TaskMetadata;
import com.linecorp.decaton.protocol.Decaton.DecatonTaskRequest;
import com.linecorp.decaton.protocol.Decaton.TaskMetadataProto;
import com.linecorp.decaton.protocol.Sample.HelloTask;

public class ProcessingContextImplTest {
    private static class MockCompletion implements DeferredCompletion {
        @Override
        public void complete() {
            // noop
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
    private DecatonProcessor<HelloTask> processorMock;

    private static void terminateExecutor(ExecutorService executor) throws InterruptedException {
        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
    }

    @SafeVarargs
    private static ProcessingContextImpl<HelloTask> context(DecatonProcessor<HelloTask>... processors) {
        TaskRequest request = new TaskRequest(
                new TopicPartition("topic", 1), 1, null, "TEST", REQUEST.toByteArray());
        DecatonTask<HelloTask> task = new DecatonTask<>(
                TaskMetadata.fromProto(REQUEST.getMetadata()), TASK, TASK.toByteArray());
        return new ProcessingContextImpl<>("subscription", request, task, Arrays.asList(processors), null);
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
        ProcessingContextImpl<HelloTask> context = context((ctx, task) -> { /* noop */ });

        CompletableFuture<Void> future = context.push(TASK);
        assertTrue(future.isDone());
    }

    @Test(timeout = 5000)
    public void testPush_Level1_Async() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();

        ProcessingContextImpl<HelloTask> context = context((ctx, task) -> {
            DeferredCompletion comp = ctx.deferCompletion();
            executor.execute(() -> {
                safeAwait(latch);
                comp.complete();
            });
        });

        CompletableFuture<Void> future = context.push(TASK);
        assertFalse(future.isDone());

        latch.countDown();
        terminateExecutor(executor);
        assertTrue(future.isDone());
    }

    /**
     * This test is more a likely testing usage of {@link CompletableFuture} than a logic of
     * {@link ProcessingContextImpl} but still we want to keep it to make sure that the our processing model
     * works consistently to what we expect.
     */
    @Test(timeout = 5000)
    public void testPush_Level1_MultiPush_BothSync() throws InterruptedException {
        ProcessingContextImpl<HelloTask> context = context((ctx, task) -> { /* noop */ });

        CompletableFuture<Void> f1 = context.push(TASK);
        CompletableFuture<Void> f2 = context.push(TASK);
        CompletableFuture<Void> fAll = CompletableFuture.allOf(f1, f2);

        assertTrue(f1.isDone());
        assertTrue(f2.isDone());
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

        ProcessingContextImpl<HelloTask> context = context((ctx, task) -> {
            DeferredCompletion comp = ctx.deferCompletion();
            int i = processCount.getAndIncrement();
            executors[i].execute(() -> {
                safeAwait(latches[i]);
                comp.complete();
            });
        });

        CompletableFuture<Void> f1 = context.push(TASK);
        CompletableFuture<Void> f2 = context.push(TASK);
        CompletableFuture<Void> fAll = CompletableFuture.allOf(f1, f2);

        assertFalse(fAll.isDone());
        latches[0].countDown();
        terminateExecutor(executors[0]);
        assertTrue(f1.isDone());
        assertFalse(fAll.isDone());
        latches[1].countDown();
        terminateExecutor(executors[1]);
        assertTrue(f2.isDone());
        assertTrue(fAll.isDone());
    }

    @Test(timeout = 5000)
    public void testPush_Level2_Sync() throws InterruptedException {
        ProcessingContextImpl<HelloTask> context = context(ProcessingContext::push, processorMock);

        CompletableFuture<Void> future = context.push(TASK);
        assertTrue(future.isDone());
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

        ProcessingContextImpl<HelloTask> context = context(ProcessingContext::push, processorMock);

        CompletableFuture<Void> future = context.push(TASK);
        assertTrue(future.isDone());
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

        ProcessingContextImpl<HelloTask> context = context(
                (ctx, task) -> ctx.deferCompletion().completeWith(ctx.push(task)),
                processorMock);

        CompletableFuture<Void> future = context.push(TASK);
        assertFalse(future.isDone());
        verify(processorMock, times(1)).process(any(), eq(TASK));

        latch.countDown();
        terminateExecutor(executor);
        assertTrue(future.isDone());
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

        ProcessingContextImpl<HelloTask> context = context(
                (ctx, task) -> ctx.deferCompletion().completeWith(ctx.push(TASK)), processorMock);

        CompletableFuture<Void> f1 = context.push(TASK);
        CompletableFuture<Void> f2 = context.push(TASK);
        CompletableFuture<Void> fAll = CompletableFuture.allOf(f1, f2);

        assertTrue(f1.isDone());
        assertFalse(f2.isDone());
        assertFalse(fAll.isDone());

        latch.countDown();
        terminateExecutor(executor);
        assertTrue(f1.isDone());
        assertTrue(f2.isDone());
        assertTrue(fAll.isDone());
    }

    @Test
    public void testRetry() throws InterruptedException {
        @SuppressWarnings("unchecked")
        DecatonProcessor<byte[]> retryProcessor = mock(DecatonProcessor.class);
        DeferredCompletion completion = spy(new MockCompletion());

        TaskRequest request = new TaskRequest(
                new TopicPartition("topic", 1), 1, null, "TEST", REQUEST.toByteArray());
        DecatonTask<byte[]> task = new DecatonTask<>(
                TaskMetadata.fromProto(REQUEST.getMetadata()), TASK.toByteArray(), TASK.toByteArray());

        ProcessingContextImpl<byte[]> context =
                spy(new ProcessingContextImpl<>("subscription", request, task, completion,
                                                Collections.emptyList(), retryProcessor));

        CompletableFuture<Void> produceFuture = context.retry();

        verify(context, times(1)).deferCompletion();
        verify(retryProcessor, times(1)).process(any(), eq(TASK.toByteArray()));
        verify(completion, times(1)).complete();
        assertTrue(produceFuture.isDone());
    }

    @Test(expected = IllegalStateException.class)
    public void testRetry_NOT_CONFIGURED() throws InterruptedException {
        context().retry();
    }
}
