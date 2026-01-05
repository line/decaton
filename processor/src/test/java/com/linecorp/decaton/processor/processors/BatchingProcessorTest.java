/*
 * Copyright 2021 LINE Corporation
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.linecorp.decaton.processor.Completion;
import com.linecorp.decaton.processor.ProcessingContext;
import com.linecorp.decaton.protocol.Sample.HelloTask;

@ExtendWith(MockitoExtension.class)
public class BatchingProcessorTest {
    @Mock
    private ProcessingContext<HelloTask> context;

    @Mock
    private Completion completion;

    private final List<HelloTask> processedTasks = Collections.synchronizedList(new ArrayList<>());

    @BeforeEach
    public void before() {
        processedTasks.clear();
    }

    private HelloTask buildHelloTask(String name, int age) {
        return HelloTask.newBuilder().setName(name).setAge(age).build();
    }

    private BatchingProcessor<HelloTask> buildProcessor(CountDownLatch processLatch,
                                                        Supplier<Long> lingerMs,
                                                        Supplier<Integer> capacity) {
        return new BatchingProcessor<HelloTask>(lingerMs, capacity) {
            @Override
            protected void processBatchingTasks(List<BatchingTask<HelloTask>> batchingTasks) {
                List<HelloTask> helloTasks =
                        batchingTasks.stream().map(BatchingTask::task).collect(Collectors.toList());
                processedTasks.addAll(helloTasks);
                batchingTasks.forEach(batchingTask -> batchingTask.completion().complete());
                processLatch.countDown();
            }
        };
    }

    @Test
    @Timeout(5)
    public void testLingerLimit() throws InterruptedException {
        doReturn(completion).when(context).deferCompletion();

        long lingerMs = 1000;
        CountDownLatch processLatch = new CountDownLatch(1);
        BatchingProcessor<HelloTask> processor = buildProcessor(processLatch, () -> lingerMs, () -> Integer.MAX_VALUE);

        HelloTask task1 = buildHelloTask("one", 1);
        processor.process(context, task1);

        processLatch.await();

        assertEquals(Collections.singletonList(task1), processedTasks);
        verify(context, times(1)).deferCompletion();
        verify(completion, times(1)).complete();
    }

    @Test
    public void testCapacityLimit() throws InterruptedException {
        doReturn(completion).when(context).deferCompletion();

        CountDownLatch processLatch = new CountDownLatch(1);
        BatchingProcessor<HelloTask> processor = buildProcessor(processLatch, () -> Long.MAX_VALUE, () -> 2);

        HelloTask task1 = buildHelloTask("one", 1);
        HelloTask task2 = buildHelloTask("two", 2);
        HelloTask task3 = buildHelloTask("three", 3);

        processor.process(context, task1);
        processor.process(context, task2);
        processor.process(context, task3);

        processLatch.await();

        // linger-based flush is disabled, so we can expect second batch never be processed here
        assertEquals(new ArrayList<>(Arrays.asList(task1, task2)), processedTasks);
        verify(context, times(3)).deferCompletion();
        verify(completion, times(processedTasks.size())).complete();
    }

    @Test
    public void failFastDuringInstantiation() {
        assertThrows(NullPointerException.class,
                     () -> buildProcessor(new CountDownLatch(1), null, null));
        assertThrows(IllegalArgumentException.class,
                     () -> buildProcessor(new CountDownLatch(1), () -> 0L, () -> 1));
        assertThrows(IllegalArgumentException.class,
                     () -> buildProcessor(new CountDownLatch(1), () -> 100L, () -> 0));
        assertThrows(IllegalArgumentException.class,
                     () -> buildProcessor(new CountDownLatch(1), () -> 100L / 0, () -> -1));
    }

    @Test
    public void fallbackToLKG() throws InterruptedException {
        doReturn(completion).when(context).deferCompletion();

        // only 2 is a legal value, and used as lask-known-good fallback.
        final int[] capacityValues = {2, 0, -1};
        AtomicInteger count = new AtomicInteger(0);
        CountDownLatch processLatch = new CountDownLatch(1);
        BatchingProcessor<HelloTask> processor =
                buildProcessor(processLatch,
                               () -> Long.MAX_VALUE,
                               () -> capacityValues[count.getAndIncrement()
                                                    % capacityValues.length]);

        HelloTask task1 = buildHelloTask("one", 1);
        HelloTask task2 = buildHelloTask("two", 2);
        HelloTask task3 = buildHelloTask("three", 3);

        processor.process(context, task1);
        processor.process(context, task2);
        processor.process(context, task3);

        processLatch.await();

        assertEquals(new ArrayList<>(Arrays.asList(task1, task2)), processedTasks);
        verify(context, times(3)).deferCompletion();
        verify(completion, times(processedTasks.size())).complete();
        assertEquals(4, count.get());
    }
}
