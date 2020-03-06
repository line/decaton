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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.function.Supplier;

import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

import com.linecorp.decaton.processor.DecatonProcessor;
import com.linecorp.decaton.processor.ProcessingContext;
import com.linecorp.decaton.protocol.Sample.HelloTask;

/**
 * This unit tests practically tests not only the custom scopes developed in this project, but also
 * Sprint itself's behavior to confirm our expectation is correct and for potential future breaking changes.
 */
public class SpringProcessorSupplierTest {
    @Rule
    public MockitoRule rule = MockitoJUnit.rule();

    private static class TheProcessor implements DecatonProcessor<HelloTask> {
        private final AutoCloseable singletonScoped;
        private final AutoCloseable partitionScoped;
        private final AutoCloseable subpartitionScoped;

        private TheProcessor(AutoCloseable singletonScoped, AutoCloseable partitionScoped,
                             AutoCloseable subpartitionScoped) {
            this.singletonScoped = singletonScoped;
            this.partitionScoped = partitionScoped;
            this.subpartitionScoped = subpartitionScoped;
        }

        @Override
        public void process(ProcessingContext<HelloTask> context, HelloTask task) throws InterruptedException {
            // noop
        }
    }

    @Configuration
    public static class TheConfiguration {
        @Bean
        public AutoCloseable singletonScoped() {
            return mock(AutoCloseable.class);
        }

        @Bean
        @Scope(DecatonSpring.PARTITION_SCOPE)
        public AutoCloseable partitionScoped() {
            return mock(AutoCloseable.class);
        }

        @Bean
        @Scope(DecatonSpring.SUBPARTITION_SCOPE)
        public AutoCloseable subpartitionScoped() {
            return mock(AutoCloseable.class);
        }

        @Bean
        @Scope(DecatonSpring.PARTITION_SCOPE)
        public TheProcessor partitionProcessor(AutoCloseable singletonScoped, AutoCloseable partitionScoped) {
            return spy(new TheProcessor(singletonScoped, partitionScoped, null));
        }

        @Bean
        @Scope(DecatonSpring.SUBPARTITION_SCOPE)
        public TheProcessor subpartitionProcessor(AutoCloseable singletonScoped, AutoCloseable partitionScoped,
                                                  AutoCloseable subpartitionScoped) {
            return spy(new TheProcessor(singletonScoped, partitionScoped, subpartitionScoped));
        }
    }

    @Spy
    private final PartitionScope partitionScope = new PartitionScope();

    @Spy
    private final SubpartitionScope subpartitionScope = new SubpartitionScope();

    private AnnotationConfigApplicationContext appContext;

    private SpringProcessorSupplier<HelloTask> partitionProcessorSupplier;

    private SpringProcessorSupplier<HelloTask> subpartitionProcessorSupplier;

    @Before
    public void setUp() {
        doReturn("subscription").when(partitionScope).processingSubscription();

        appContext = new AnnotationConfigApplicationContext();
        appContext.getBeanFactory().registerScope(DecatonSpring.PARTITION_SCOPE, partitionScope);
        appContext.getBeanFactory().registerScope(DecatonSpring.SUBPARTITION_SCOPE, subpartitionScope);
        appContext.register(TheConfiguration.class);
        appContext.refresh();

        partitionProcessorSupplier = new SpringProcessorSupplier<>(
                beanSupplier(appContext, "partitionProcessor"),
                appContext);
        subpartitionProcessorSupplier = new SpringProcessorSupplier<>(
                beanSupplier(appContext, "subpartitionProcessor"), appContext);
    }

    @SuppressWarnings("unchecked")
    private static <T> Supplier<T> beanSupplier(ConfigurableApplicationContext appContext, String beanName) {
        return () -> (T) appContext.getBean(beanName);
    }

    private static TopicPartition tp(int partition) {
        return new TopicPartition("topic", partition);
    }

    private TheProcessor getPartitionProcessor(int partition, int threadId) {
        return (TheProcessor) partitionProcessorSupplier.getProcessor("s", tp(partition), threadId);
    }

    private TheProcessor getSubpartitionProcessor(int partition, int threadId) {
        return (TheProcessor) subpartitionProcessorSupplier.getProcessor("s", tp(partition), threadId);
    }

    @Test
    public void testGetBean_SamePartition() {
        TheProcessor bean1 = getPartitionProcessor(1, 0);
        TheProcessor bean2 = getPartitionProcessor(1, 1);

        assertSame(bean1, bean2);
    }

    @Test
    public void testGetBean_DifferentPartition() {
        TheProcessor bean1 = getPartitionProcessor(1, 0);
        TheProcessor bean2 = getPartitionProcessor(2, 0);

        assertNotSame(bean1, bean2);
    }

    @Test
    public void testGetBean_SameSubpartition() {
        TheProcessor bean1 = getSubpartitionProcessor(1, 1);
        TheProcessor bean2 = getSubpartitionProcessor(1, 1);

        assertSame(bean1, bean2);
    }

    @Test
    public void testGetBean_DifferentSubpartition() {
        TheProcessor bean1 = getSubpartitionProcessor(1, 1);
        TheProcessor bean2 = getSubpartitionProcessor(1, 2);

        assertNotSame(bean1, bean2);
    }

    @Test
    public void testDestruction_PartitionScoped() throws Exception {
        TheProcessor bean1 = getPartitionProcessor(1, 0);
        TheProcessor bean2 = getPartitionProcessor(2, 0);

        // destroy only bean1
        partitionProcessorSupplier.leavePartitionScope("s", tp(1));

        verify(bean1, times(1)).close();
        verify(bean2, times(0)).close();
        // Close shouldn't be called for singleton object
        verify(bean1.singletonScoped, times(0)).close();
        // Close should have been called for partition scoped inner object
        verify(bean1.partitionScoped, times(1)).close();
        // But not for one which is held by bean2
        verify(bean2.partitionScoped, times(0)).close();
    }

    @Test
    public void testDestruction_SubpartitionScoped() throws Exception {
        TheProcessor bean1 = getSubpartitionProcessor(1, 1);
        TheProcessor bean2 = getSubpartitionProcessor(1, 2);

        // destroy only bean1
        partitionProcessorSupplier.leaveThreadScope("s", tp(1), 1);

        verify(bean1, times(1)).close();
        verify(bean2, times(0)).close();
        // Close shouldn't be called for singleton object
        verify(bean1.singletonScoped, times(0)).close();
        // Close should have been called for partition scoped inner object
        verify(bean1.partitionScoped, times(0)).close();
        // Close should have been called for subpartition scoped inner object
        verify(bean1.subpartitionScoped, times(1)).close();
        // But not for one which is held by bean2
        verify(bean2.subpartitionScoped, times(0)).close();
    }

    @Test
    public void testDestroyingAppContext() throws Exception {
        TheProcessor partitionBean = getPartitionProcessor(1, 0);
        TheProcessor subpartitionBean = getSubpartitionProcessor(1, 1);

        appContext.close();
        verify(partitionBean.singletonScoped, times(1)).close();
        // Even if the appContext got destroyed scoped objects are kept untouched.
        verify(partitionBean, times(0)).close();
        verify(partitionBean.partitionScoped, times(0)).close();

        verify(subpartitionBean.singletonScoped, times(1)).close();
        // Even if the appContext got destroyed scoped objects are kept untouched.
        verify(subpartitionBean, times(0)).close();
        verify(subpartitionBean.partitionScoped, times(0)).close();
        verify(subpartitionBean.subpartitionScoped, times(0)).close();
    }
}
