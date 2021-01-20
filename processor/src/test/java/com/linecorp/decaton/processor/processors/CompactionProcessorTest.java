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

package com.linecorp.decaton.processor.processors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import org.apache.kafka.common.TopicPartition;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import com.linecorp.decaton.processor.DecatonProcessor;
import com.linecorp.decaton.processor.runtime.DecatonTask;
import com.linecorp.decaton.processor.DeferredCompletion;
import com.linecorp.decaton.processor.ProcessingContext;
import com.linecorp.decaton.processor.runtime.TaskMetadata;
import com.linecorp.decaton.processor.processors.CompactionProcessor.CompactChoice;
import com.linecorp.decaton.processor.processors.CompactionProcessor.CompactingTask;
import com.linecorp.decaton.processor.runtime.internal.ProcessingContextImpl;
import com.linecorp.decaton.processor.runtime.internal.TaskRequest;
import com.linecorp.decaton.protocol.Sample.HelloTask;

public class CompactionProcessorTest {
    @Rule
    public MockitoRule rule = MockitoJUnit.rule();

    private static final long LINGER_MS = 1000;

    private static class TaskInput {
        final HelloTask task;
        final DeferredCompletion completion;
        final ProcessingContext<HelloTask> context;

        private TaskInput(HelloTask task, DeferredCompletion completion,
                          ProcessingContext<HelloTask> context) {
            this.task = task;
            this.completion = completion;
            this.context = context;
        }
    }

    @Spy
    private final CompactionProcessor<HelloTask> processor =
            new CompactionProcessor<>(LINGER_MS, (val1, val2) -> {
                if (val1.task().getAge() == val2.task().getAge()) {
                    return CompactChoice.PICK_EITHER;
                } else if (val1.task().getAge() > val2.task().getAge()) {
                    return CompactChoice.PICK_LEFT;
                } else {
                    return CompactChoice.PICK_RIGHT;
                }
            });

    @Mock
    private DecatonProcessor<HelloTask> downstream;

    private static class NoopDeferredCompletion implements DeferredCompletion {
        @Override
        public void complete() {
            // noop
        }
    }

    private TaskInput put(DecatonProcessor<HelloTask> processor,
                          String name, int age,
                          Consumer<TaskInput> beforeProcess) throws InterruptedException {
        // Can't use mock() for this to keep completeWith default method functioning
        DeferredCompletion completion = spy(new NoopDeferredCompletion());

        HelloTask taskData = HelloTask.newBuilder().setName(name).setAge(age).build();
        DecatonTask<HelloTask> task = new DecatonTask<>(
                TaskMetadata.builder().build(),
                taskData,
                taskData.toByteArray());
        TaskRequest request = new TaskRequest(
                new TopicPartition("topic", 1), 1, null, name, null, null, null);
        ProcessingContext<HelloTask> context =
                Mockito.spy(new ProcessingContextImpl<>("subscription", request, task, completion,
                                                        Collections.singletonList(downstream),
                                                        null));

        TaskInput input = new TaskInput(taskData, completion, context);
        if (beforeProcess != null) {
            beforeProcess.accept(input);
        }
        processor.process(context, taskData);
        return input;
    }

    private TaskInput put(String name, int age, Consumer<TaskInput> beforeProcess) throws InterruptedException {
        return put(processor, name, age, beforeProcess);
    }

    private TaskInput put(String name, int age) throws InterruptedException {
        return put(name, age, null);
    }

    @Test(timeout = 5000)
    public void testCompactedOutput() throws InterruptedException {
        CountDownLatch waitFlush = new CountDownLatch(1);
        CountDownLatch completeFlush = new CountDownLatch(3);

        doAnswer(invocation -> {
            Runnable original = (Runnable) invocation.callRealMethod();
            return (Runnable) () -> {
                try {
                    waitFlush.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                original.run();
                completeFlush.countDown();
            };
        }).when(processor).flushTask(any());

        TaskInput youngYuto = put("yuto", 10);
        TaskInput dyingYuto = put("yuto", 90);
        TaskInput wonpill = put("wonpill", 400);
        TaskInput oldYuto = put("yuto", 20);

        waitFlush.countDown();

        Thread.sleep(LINGER_MS * 2); // doubling just to make sure flush completed in background
        TaskInput babyYuto = put("yuto", 1);

        completeFlush.await();

        // Those tasks are compacted into dyingYuto
        verify(youngYuto.context, never()).push(any());
        verify(oldYuto.context, never()).push(any());

        // Those tasks are the compaction results
        verify(dyingYuto.context, times(1)).push(dyingYuto.task);
        verify(wonpill.context, times(1)).push(wonpill.task);

        // This task should be processed independently as it had produced after compaction window
        verify(babyYuto.context, times(1)).push(babyYuto.task);
    }

    @Test(timeout = 5000)
    public void testOutputDelayed() throws InterruptedException {
        CountDownLatch completeFlush = new CountDownLatch(2);

        doAnswer(invocation -> {
            Runnable original = (Runnable) invocation.callRealMethod();
            return (Runnable) () -> {
                original.run();
                completeFlush.countDown();
            };
        }).when(processor).flushTask(any());

        long firstPutTime = System.nanoTime();
        put("yuto", 10);

        Thread.sleep(500);

        long secondPutTime = System.nanoTime();
        AtomicLong yutoFlushedTime = new AtomicLong();
        put("yuto", 20, input -> {
            try {
                doAnswer(invocation -> {
                    yutoFlushedTime.set(System.nanoTime());
                    return invocation.callRealMethod();
                }).when(input.context).push(input.task);
            } catch (InterruptedException ignored) {
                // impossible
            }
        });
        AtomicLong wonpillFlushedTime = new AtomicLong();
        put("wonpill", 400, input -> {
            try {
                doAnswer(invocation -> {
                    wonpillFlushedTime.set(System.nanoTime());
                    return invocation.callRealMethod();
                }).when(input.context).push(input.task);
            } catch (InterruptedException ignored) {
                // impossible
            }
        });

        completeFlush.await();

        assertTrue(yutoFlushedTime.get() >= firstPutTime + TimeUnit.MILLISECONDS.toNanos(LINGER_MS));
        assertTrue(yutoFlushedTime.get() < secondPutTime + TimeUnit.MILLISECONDS.toNanos(LINGER_MS));
        assertTrue(wonpillFlushedTime.get() >= secondPutTime + TimeUnit.MILLISECONDS.toNanos(LINGER_MS));
    }

    @Test(timeout = 5000)
    public void testCompletionHandling() throws InterruptedException {
        // In this test downstream processor defers task's completion and never completes it.
        doAnswer(invocation -> {
            ProcessingContext<?> context = invocation.getArgument(0);
            context.deferCompletion();
            return null;
        }).when(downstream).process(any(), any());

        CountDownLatch waitFlush = new CountDownLatch(1);
        CountDownLatch completeFlush = new CountDownLatch(1);

        doAnswer(invocation -> {
            Runnable original = (Runnable) invocation.callRealMethod();
            return (Runnable) () -> {
                try {
                    waitFlush.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                original.run();
                completeFlush.countDown();
            };
        }).when(processor).flushTask(any());

        TaskInput youngYuto = put("yuto", 10);
        verify(youngYuto.completion, never()).complete();

        TaskInput dyingYuto = put("yuto", 90);
        verify(youngYuto.completion, times(1)).complete();
        verify(dyingYuto.completion, never()).complete();

        TaskInput oldYuto = put("yuto", 20);
        verify(dyingYuto.completion, never()).complete();
        verify(oldYuto.completion, times(1)).complete();

        waitFlush.countDown();
        completeFlush.await();

        // CompactionProcessor should never complete a task pushed to downstream processor.
        // It's completion should be handled by the downstream processor either synchronously or asynchronously.
        verify(dyingYuto.completion, never()).complete();
        verify(dyingYuto.context, times(1)).push(dyingYuto.task);
    }

    @Test(timeout = 5000)
    public void testRaceConditionOnFlush() throws InterruptedException {
        CountDownLatch schedulePassed = new CountDownLatch(1);
        CountDownLatch firstFlushComplete = new CountDownLatch(1);
        CountDownLatch secondFlushComplete = new CountDownLatch(1);

        ScheduledThreadPoolExecutor scheduledExecutor = new ScheduledThreadPoolExecutor(1);
        BiFunction<CompactingTask<HelloTask>, CompactingTask<HelloTask>, CompactChoice> compactor =
                (t1, t2) -> {
                    schedulePassed.countDown();
                    try {
                        firstFlushComplete.await();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    return CompactChoice.PICK_RIGHT;
                };
        CompactionProcessor<HelloTask> processor = spy(
                new CompactionProcessor<>(1, compactor, scheduledExecutor));

        AtomicInteger flushCount = new AtomicInteger();
        doAnswer(invocation -> {
            Runnable original = (Runnable) invocation.callRealMethod();
            int count = flushCount.getAndIncrement();
            return (Runnable) () -> {
                if (count == 0) {
                    try {
                        schedulePassed.await();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    original.run(); // the key removed from windowedTasks inside this call.
                    firstFlushComplete.countDown();
                } else {
                    original.run();
                    secondFlushComplete.countDown();
                }
            };
        }).when(processor).flushTask(any());

        TaskInput task1 = put(processor, "key", 1, null);
        TaskInput task2 = put(processor, "key", 2, null);

        firstFlushComplete.await();
        verify(task1.completion, times(1)).complete();
        secondFlushComplete.await();
        verify(task2.completion, times(1)).complete();

        // There must have been two separate tasks scheduled for each task.
        // We have to terminate the executor before getting task count since getTaskCount returns approximate
        // count and while it's running it seems like possible to return an unexpected value.
        scheduledExecutor.shutdown();
        scheduledExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        assertEquals(2, scheduledExecutor.getTaskCount());
    }
}
