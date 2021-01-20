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

import java.util.Set;
import java.util.function.Supplier;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.config.Scope;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;

import com.linecorp.decaton.processor.DecatonProcessor;
import com.linecorp.decaton.processor.runtime.ProcessorsBuilder;
import com.linecorp.decaton.processor.runtime.internal.DecatonProcessingContext;
import com.linecorp.decaton.processor.runtime.DecatonProcessorSupplier;

/**
 * An implementation of {@link DecatonProcessorSupplier} which provides better integration with spring
 * framework.
 * <p>
 * This processor supplier constructs a {@link DecatonProcessor} by asking underlying {@link ApplicationContext}
 * to supply it and destroys scoped beans including processor when Decaton tells to that it is leaving
 * particular partition.
 * When a partition revoked from the subscription, {@link DecatonProcessorSupplier} attempts to destroy all
 * beans scoped for the revoked partition through calling
 * {@link ConfigurableBeanFactory#destroyScopedBean(String)}, instead of calling
 * {@link DecatonProcessor#close()} directly.
 *
 * @param <T> type of task
 */
public class SpringProcessorSupplier<T> implements DecatonProcessorSupplier<T> {
    private static final Logger logger = LoggerFactory.getLogger(SpringProcessorSupplier.class);

    private final ConfigurableApplicationContext appContext;
    private final Supplier<DecatonProcessor<T>> processorSupplier;

    @SuppressWarnings("unchecked")
    private static <P> DecatonProcessor<P> getProcessorBean(
            ConfigurableApplicationContext appContext, String name) {
        return appContext.getBean(name, DecatonProcessor.class);
    }

    /**
     * Creates a new instance of {@link SpringProcessorSupplier} which uses given
     * {@link ConfigurableApplicationContext} and bean name to supply processors.
     *
     * @param appContext an instance of {@link ConfigurableApplicationContext} which is used to create/destroy
     * processor bean.
     * @param beanName the name of bean to obtain {@link DecatonProcessor} from {@link ApplicationContext}.
     * @param <P> type of task to be processed by created processor.
     *
     * @return a {@link SpringProcessorSupplier} which can be passed to
     * {@link ProcessorsBuilder#thenProcess(DecatonProcessor)}.
     */
    public static <P> SpringProcessorSupplier<P> of(
            ConfigurableApplicationContext appContext, String beanName) {
        return new SpringProcessorSupplier<>(() -> getProcessorBean(appContext, beanName), appContext);
    }

    public SpringProcessorSupplier(Supplier<DecatonProcessor<T>> supplier,
                                   ConfigurableApplicationContext appContext) {
        this.appContext = appContext;
        processorSupplier = supplier;
    }

    @Override
    public DecatonProcessor<T> getProcessor(String subscriptionId, TopicPartition tp, int threadId) {
        return DecatonProcessingContext.withContext(subscriptionId, tp, threadId, processorSupplier);
    }

    private static String scopeId() {
        return String.join("/",
                           DecatonProcessingContext.processingSubscription(),
                           String.valueOf(DecatonProcessingContext.processingTopicPartition()),
                           String.valueOf(DecatonProcessingContext.processingThreadId()));
    }

    private void destroyScopedBeansRecursively(String targetScope, String[] targetBeanNames,
                                               Set<String> scopedBeanNames) {
        ConfigurableListableBeanFactory beanFactory = appContext.getBeanFactory();
        for (String beanName : targetBeanNames) {
            if (!scopedBeanNames.contains(beanName)) {
                continue;
            }
            String beanScope = beanFactory.getBeanDefinition(beanName).getScope();
            if (!targetScope.equals(beanScope)) {
                throw new IllegalStateException(String.format(
                        "bean '%s' isn't scoped for '%s'", beanName, targetScope));
            }

            String[] dependants = beanFactory.getDependentBeans(beanName);
            destroyScopedBeansRecursively(targetScope, dependants, scopedBeanNames);

            logger.debug("destroying scoped bean '{}' of {}", beanName, scopeId());
            beanFactory.destroyScopedBean(beanName);
            scopedBeanNames.remove(beanName);
        }
    }

    public void destroyScopedBeans(String scopeName) {
        ConfigurableListableBeanFactory beanFactory = appContext.getBeanFactory();

        Scope scope = beanFactory.getRegisteredScope(scopeName);
        if (!(scope instanceof AbstractDecatonScope)) {
            throw new IllegalArgumentException(String.format(
                    "scope '%s' isn't a %s", scopeName, AbstractDecatonScope.class.getSimpleName()));
        }

        AbstractDecatonScope decatonScope = (AbstractDecatonScope) scope;
        Set<String> scopedBeanNames = decatonScope.scopedBeanNames();
        if (scopedBeanNames.isEmpty()) {
            return;
        }

        String[] allBeanNames = beanFactory.getBeanDefinitionNames();
        destroyScopedBeansRecursively(scopeName, allBeanNames, scopedBeanNames);
        if (!scopedBeanNames.isEmpty()) {
            logger.warn("some beans of scope '{}' leaked on scope destruction: {}", scopeName, scopedBeanNames);
        }
    }

    @Override
    public void leaveSingletonScope(String subscriptionId) {
        // Make this handler no-op to make behavior consistent to other two scopes(partition, thread)
        // All singleton beans are supposed to be cleaned up along with application context's destruction.
    }

    @Override
    public void leavePartitionScope(String subscriptionId, TopicPartition tp) {
        DecatonProcessingContext.withContext(subscriptionId, tp, null, () -> {
            destroyScopedBeans(DecatonSpring.PARTITION_SCOPE);
            return null;
        });
    }

    @Override
    public void leaveThreadScope(String subscriptionId, TopicPartition tp, int threadId) {
        DecatonProcessingContext.withContext(subscriptionId, tp, threadId, () -> {
            destroyScopedBeans(DecatonSpring.SUBPARTITION_SCOPE);
            return null;
        });
    }
}
