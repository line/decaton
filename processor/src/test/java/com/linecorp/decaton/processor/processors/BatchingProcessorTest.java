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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import com.linecorp.decaton.processor.Completion;
import com.linecorp.decaton.processor.ProcessingContext;
import com.linecorp.decaton.protocol.Sample.HelloTask;

public class BatchingProcessorTest {
    @Rule
    public MockitoRule rule = MockitoJUnit.rule();

    @Mock
    private ProcessingContext<HelloTask> context;

    @Mock
    private Completion completion;

    private final List<HelloTask> processedTasks = new ArrayList<>();

    @Before
    public void before() {
        doReturn(completion).when(context).deferCompletion();
        processedTasks.clear();
    }

    private HelloTask buildHelloTask(String name, int age) {
        return HelloTask.newBuilder().setName(name).setAge(age).build();
    }

    private BatchingProcessor<HelloTask> buildProcessor(long lingerMs, int capacity) {
        return new BatchingProcessor<HelloTask>(lingerMs, capacity) {
            @Override
            protected void processBatchingTasks(List<BatchingTask<HelloTask>> batchingTasks) {
                batchingTasks.forEach(batchingTask -> {
                    processedTasks.add(batchingTask.task());
                    batchingTask.completion().complete();
                });
            }
        };
    }

    @Test(timeout = 5000)
    public void testLingerLimit() throws InterruptedException {
        long lingerMs = 1000;
        BatchingProcessor<HelloTask> processor = buildProcessor(lingerMs, Integer.MAX_VALUE);

        HelloTask task1 = buildHelloTask("one", 1);
        processor.process(context, task1);

        int maxLoopCount = 5;
        for (int i = 0; i < maxLoopCount; i++) {
            if (!processedTasks.isEmpty()) {
                break;
            }
            Thread.sleep(lingerMs);
        }

        assertEquals(Collections.singletonList(task1), processedTasks);
        verify(context, times(1)).deferCompletion();
        verify(completion, times(1)).complete();
    }

    @Test(timeout = 5000)
    public void testCapacityLimit() throws InterruptedException {
        BatchingProcessor<HelloTask> processor = buildProcessor(Long.MAX_VALUE, 2);

        HelloTask task1 = buildHelloTask("one", 1);
        HelloTask task2 = buildHelloTask("two", 2);
        HelloTask task3 = buildHelloTask("three", 3);
        HelloTask task4 = buildHelloTask("four", 4);
        HelloTask task5 = buildHelloTask("five", 5);

        processor.process(context, task1);
        processor.process(context, task2);
        processor.process(context, task3);
        assertEquals(Arrays.asList(task1, task2), processedTasks);
        processor.process(context, task4);
        processor.process(context, task5);
        assertEquals(Arrays.asList(task1, task2, task3, task4), processedTasks);

        verify(context, times(5)).deferCompletion();
        verify(completion, times(processedTasks.size())).complete();
    }
}
