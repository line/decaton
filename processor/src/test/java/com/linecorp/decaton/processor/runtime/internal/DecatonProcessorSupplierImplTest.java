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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import com.linecorp.decaton.processor.DecatonProcessor;
import com.linecorp.decaton.processor.runtime.ProcessorScope;
import com.linecorp.decaton.protocol.Sample.HelloTask;

public class DecatonProcessorSupplierImplTest {
    @Rule
    public MockitoRule rule = MockitoJUnit.rule();

    private static final String SUBSC_ID = "subscription";

    @Mock
    private Supplier<DecatonProcessor<HelloTask>> supplier;

    @Before
    public void setUp() {
        doAnswer(invocation -> mock(DecatonProcessor.class)).when(supplier).get();
    }

    private static TopicPartition tp(int partition) {
        return new TopicPartition("topic", partition);
    }

    private DecatonProcessorSupplierImpl<HelloTask> processorSupplier(ProcessorScope scope) {
        return new DecatonProcessorSupplierImpl<>(supplier, scope);
    }

    @Test
    public void testGetProcessor_PROVIDED() {
        DecatonProcessorSupplierImpl<HelloTask> processorSupplier = processorSupplier(ProcessorScope.PROVIDED);

        DecatonProcessor<HelloTask> p1t1 = processorSupplier.getProcessor(SUBSC_ID, tp(1), 1);
        DecatonProcessor<HelloTask> p1t2 = processorSupplier.getProcessor(SUBSC_ID, tp(2), 1);

        verify(supplier, times(1)).get();
        assertSame(p1t1, p1t2);
    }

    @Test
    public void testGetProcessor_SINGLETON() {
        DecatonProcessorSupplierImpl<HelloTask> processorSupplier = processorSupplier(ProcessorScope.SINGLETON);

        DecatonProcessor<HelloTask> p1t1 = processorSupplier.getProcessor(SUBSC_ID, tp(1), 1);
        DecatonProcessor<HelloTask> p1t2 = processorSupplier.getProcessor(SUBSC_ID, tp(2), 1);

        verify(supplier, times(1)).get();
        assertSame(p1t1, p1t2);
    }

    @Test
    public void testGetProcessor_PARTITION() {
        DecatonProcessorSupplierImpl<HelloTask> processorSupplier = processorSupplier(ProcessorScope.PARTITION);

        DecatonProcessor<HelloTask> p1t1 = processorSupplier.getProcessor(SUBSC_ID, tp(1), 1);
        DecatonProcessor<HelloTask> p1t2 = processorSupplier.getProcessor(SUBSC_ID, tp(1), 2);

        DecatonProcessor<HelloTask> p2t1 = processorSupplier.getProcessor(SUBSC_ID, tp(2), 1);
        DecatonProcessor<HelloTask> p2t2 = processorSupplier.getProcessor(SUBSC_ID, tp(2), 2);

        verify(supplier, times(2)).get();
        assertSame(p1t1, p1t2);
        assertSame(p2t1, p2t2);
        assertNotSame(p1t1, p2t1);
    }

    @Test
    public void testGetProcessor_THREAD() {
        DecatonProcessorSupplierImpl<HelloTask> processorSupplier = processorSupplier(ProcessorScope.THREAD);

        DecatonProcessor<HelloTask> p1t1 = processorSupplier.getProcessor(SUBSC_ID, tp(1), 1);
        DecatonProcessor<HelloTask> p1t1v2 = processorSupplier.getProcessor(SUBSC_ID, tp(1), 1);
        DecatonProcessor<HelloTask> p1t2 = processorSupplier.getProcessor(SUBSC_ID, tp(1), 2);
        DecatonProcessor<HelloTask> p2t1 = processorSupplier.getProcessor(SUBSC_ID, tp(2), 1);

        verify(supplier, times(3)).get();
        assertSame(p1t1, p1t1v2);
        assertNotSame(p1t1, p1t2);
        assertNotSame(p1t2, p2t1);
    }

    @Test
    public void testLeaveScope_PROVIDED() throws Exception {
        DecatonProcessorSupplierImpl<HelloTask> processorSupplier = processorSupplier(ProcessorScope.PROVIDED);

        DecatonProcessor<HelloTask> p1t1 = processorSupplier.getProcessor(SUBSC_ID, tp(1), 1);

        processorSupplier.leaveThreadScope(SUBSC_ID, tp(1), 1);
        verify(p1t1, times(0)).close();

        processorSupplier.leavePartitionScope(SUBSC_ID, tp(1));
        verify(p1t1, times(0)).close();

        processorSupplier.leaveSingletonScope(SUBSC_ID);
        verify(p1t1, times(0)).close();
    }

    @Test
    public void testLeaveScope_SINGLETON() throws Exception {
        DecatonProcessorSupplierImpl<HelloTask> processorSupplier = processorSupplier(ProcessorScope.SINGLETON);

        DecatonProcessor<HelloTask> p1t1 = processorSupplier.getProcessor(SUBSC_ID, tp(1), 1);

        processorSupplier.leaveThreadScope(SUBSC_ID, tp(1), 1);
        verify(p1t1, times(0)).close();

        processorSupplier.leavePartitionScope(SUBSC_ID, tp(1));
        verify(p1t1, times(0)).close();

        processorSupplier.leaveSingletonScope(SUBSC_ID);
        verify(p1t1, times(1)).close();
    }

    @Test
    public void testLeaveScope_PARTITION() throws Exception {
        DecatonProcessorSupplierImpl<HelloTask> processorSupplier = processorSupplier(ProcessorScope.PARTITION);

        DecatonProcessor<HelloTask> p1t1 = processorSupplier.getProcessor(SUBSC_ID, tp(1), 1);
        DecatonProcessor<HelloTask> p2t1 = processorSupplier.getProcessor(SUBSC_ID, tp(2), 1);

        processorSupplier.leaveThreadScope(SUBSC_ID, tp(1), 1);
        verify(p1t1, times(0)).close();

        processorSupplier.leaveSingletonScope(SUBSC_ID);
        verify(p1t1, times(0)).close();
        verify(p2t1, times(0)).close();

        processorSupplier.leavePartitionScope(SUBSC_ID, tp(1));
        verify(p1t1, times(1)).close();
        verify(p2t1, times(0)).close();
    }

    @Test
    public void testLeaveScope_THREAD() throws Exception {
        DecatonProcessorSupplierImpl<HelloTask> processorSupplier = processorSupplier(ProcessorScope.THREAD);

        DecatonProcessor<HelloTask> p1t1 = processorSupplier.getProcessor(SUBSC_ID, tp(1), 1);
        DecatonProcessor<HelloTask> p1t2 = processorSupplier.getProcessor(SUBSC_ID, tp(1), 2);
        DecatonProcessor<HelloTask> p2t1 = processorSupplier.getProcessor(SUBSC_ID, tp(2), 1);

        processorSupplier.leavePartitionScope(SUBSC_ID, tp(1));
        verify(p1t1, times(0)).close();

        processorSupplier.leaveSingletonScope(SUBSC_ID);
        verify(p1t1, times(0)).close();

        processorSupplier.leaveThreadScope(SUBSC_ID, tp(1), 1);
        verify(p1t1, times(1)).close();
        verify(p1t2, times(0)).close();
        verify(p2t1, times(0)).close();
    }

    @Test
    public void testLeaveScopeDoubleCalling() throws Exception {
        DecatonProcessorSupplierImpl<HelloTask> processorSupplier = processorSupplier(ProcessorScope.SINGLETON);

        DecatonProcessor<HelloTask> p1t1 = processorSupplier.getProcessor(SUBSC_ID, tp(1), 1);

        // just make sure double calling leaveXX doesn't do anything unexpected.
        processorSupplier.leaveSingletonScope(SUBSC_ID);
        processorSupplier.leaveSingletonScope(SUBSC_ID);

        verify(p1t1, times(1)).close();
    }

    @Test
    public void testProcessorSupplierReceivesContext() {
        DecatonProcessorSupplierImpl<HelloTask> processorSupplier = processorSupplier(ProcessorScope.PARTITION);

        AtomicReference<String> caughtSubscriptionId = new AtomicReference<>();
        doAnswer(invocation -> {
            caughtSubscriptionId.set(DecatonProcessingContext.processingSubscription());
            return null;
        }).when(supplier).get();

        processorSupplier.getProcessor(SUBSC_ID, tp(1), 1);
        assertEquals(SUBSC_ID, caughtSubscriptionId.get());
    }

    @Test
    public void testProcessorSupplierReceivesContext_PARTITION() {
        DecatonProcessorSupplierImpl<HelloTask> processorSupplier = processorSupplier(ProcessorScope.PARTITION);

        AtomicReference<TopicPartition> caughtTp = new AtomicReference<>();
        doAnswer(invocation -> {
            caughtTp.set(DecatonProcessingContext.processingTopicPartition());
            return null;
        }).when(supplier).get();

        processorSupplier.getProcessor(SUBSC_ID, tp(1), 1);
        assertEquals(tp(1), caughtTp.get());
    }

    @Test
    public void testProcessorSupplierReceivesContext_THREAD() {
        DecatonProcessorSupplierImpl<HelloTask> processorSupplier = processorSupplier(ProcessorScope.THREAD);

        AtomicReference<TopicPartition> caughtTp = new AtomicReference<>();
        AtomicInteger caughtThreadId = new AtomicInteger();
        doAnswer(invocation -> {
            caughtTp.set(DecatonProcessingContext.processingTopicPartition());
            caughtThreadId.set(DecatonProcessingContext.processingThreadId());
            return null;
        }).when(supplier).get();

        processorSupplier.getProcessor(SUBSC_ID, tp(1), 1);
        assertEquals(tp(1), caughtTp.get());
        assertEquals(1, caughtThreadId.get());
    }

    @Test
    public void testProcessorCloseReceivesContext_PARTITION() throws Exception {
        DecatonProcessorSupplierImpl<HelloTask> processorSupplier = processorSupplier(ProcessorScope.PARTITION);

        DecatonProcessor<HelloTask> processor = processorSupplier.getProcessor(SUBSC_ID, tp(1), 1);

        AtomicReference<TopicPartition> caughtTp = new AtomicReference<>();
        doAnswer(invocation -> {
            caughtTp.set(DecatonProcessingContext.processingTopicPartition());
            return null;
        }).when(processor).close();

        processorSupplier.leavePartitionScope(SUBSC_ID, tp(1));
        assertEquals(tp(1), caughtTp.get());
    }

    @Test
    public void testProcessorCloseReceivesContext_THREAD() throws Exception {
        DecatonProcessorSupplierImpl<HelloTask> processorSupplier = processorSupplier(ProcessorScope.THREAD);

        DecatonProcessor<HelloTask> processor = processorSupplier.getProcessor(SUBSC_ID, tp(1), 1);

        AtomicReference<TopicPartition> caughtTp = new AtomicReference<>();
        AtomicInteger caughtThreadId = new AtomicInteger();
        doAnswer(invocation -> {
            caughtTp.set(DecatonProcessingContext.processingTopicPartition());
            caughtThreadId.set(DecatonProcessingContext.processingThreadId());
            return null;
        }).when(processor).close();

        processorSupplier.leaveThreadScope(SUBSC_ID, tp(1), 1);
        assertEquals(tp(1), caughtTp.get());
        assertEquals(1, caughtThreadId.get());
    }
}
