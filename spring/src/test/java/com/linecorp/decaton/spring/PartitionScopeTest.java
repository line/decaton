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

package com.linecorp.decaton.spring;

import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.springframework.beans.factory.ObjectFactory;

public class PartitionScopeTest {
    @Rule
    public MockitoRule rule = MockitoJUnit.rule();

    @Mock
    private ObjectFactory<?> objectFactory;

    @Spy
    private final PartitionScope scope = new PartitionScope();

    @Before
    public void setUp() {
        doReturn("subscription").when(scope).processingSubscription();
        doAnswer(invocation -> new Object()).when(objectFactory).getObject();
    }

    private static TopicPartition tp(int partition) {
        return new TopicPartition("topic", partition);
    }

    @Test
    public void testGet_SamePartition() {
        doReturn(tp(1)).when(scope).processingTopicPartition();

        Object bean1 = scope.get("bean", objectFactory);
        Object bean2 = scope.get("bean", objectFactory);

        verify(objectFactory, times(1)).getObject();
        assertSame(bean1, bean2);
    }

    @Test
    public void testGet_DifferentPartition() {
        doReturn(tp(1)).when(scope).processingTopicPartition();
        Object bean1 = scope.get("bean", objectFactory);
        doReturn(tp(2)).when(scope).processingTopicPartition();
        Object bean2 = scope.get("bean", objectFactory);

        verify(objectFactory, times(2)).getObject();
        assertNotSame(bean1, bean2);
    }

    @Test
    public void testRemove() {
        doReturn(tp(1)).when(scope).processingTopicPartition();
        Object bean11 = scope.get("bean", objectFactory);
        doReturn(tp(2)).when(scope).processingTopicPartition();
        Object bean21 = scope.get("bean", objectFactory);

        doReturn(tp(1)).when(scope).processingTopicPartition();
        scope.remove("bean");

        doReturn(tp(1)).when(scope).processingTopicPartition();
        Object bean12 = scope.get("bean", objectFactory);
        doReturn(tp(2)).when(scope).processingTopicPartition();
        Object bean22 = scope.get("bean", objectFactory);

        verify(objectFactory, times(3)).getObject();
        assertNotSame(bean11, bean12);
        assertSame(bean21, bean22);
    }
}
