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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectFactory;
import org.springframework.beans.factory.config.Scope;

import com.linecorp.decaton.processor.runtime.internal.DecatonProcessingContext;

abstract class AbstractDecatonScope implements Scope {
    private static final Logger logger = LoggerFactory.getLogger(AbstractDecatonScope.class);

    private static final String SCOPED_BEAN_NAME_DELIMITER = "/";

    private final ConcurrentMap<String, Object> scopedBeans;

    protected AbstractDecatonScope() {
        scopedBeans = new ConcurrentHashMap<>();
    }

    // visible and non-static for testing
    String processingSubscription() {
        return DecatonProcessingContext.processingSubscription();
    }

    // visible and non-static for testing
    TopicPartition processingTopicPartition() {
        return DecatonProcessingContext.processingTopicPartition();
    }

    // visible and non-static for testing
    int processingSubpartition() {
        return DecatonProcessingContext.processingThreadId();
    }

    protected abstract String scopeId();

    private String scopedBeanId(String beanName) {
        return scopeId() + SCOPED_BEAN_NAME_DELIMITER + beanName;
    }

    public Set<String> scopedBeanNames() {
        Set<String> scopedBeanNames = new HashSet<>();
        String scopePrefix = scopeId() + SCOPED_BEAN_NAME_DELIMITER;
        for (String scopedId : scopedBeans.keySet()) {
            if (scopedId.startsWith(scopePrefix)) {
                scopedBeanNames.add(scopedId.substring(scopePrefix.length()));
            }
        }
        return scopedBeanNames;
    }

    @Override
    public Object get(String name, ObjectFactory<?> objectFactory) {
        String scopedId = scopedBeanId(name);
        // Can't do this with null check because of a bean is allowed to be null.
        // This also can't be done by Map#computeIfAbsent because of two reasons:
        // - computeIfAbsent states computation shouldn't take long time, but bean creation tends to take long.
        // - If a bean creation involves another bean's creation which calls this method to register an another
        //   bean, nested call of this method causes deadlock.
        if (!scopedBeans.containsKey(scopedId)) {
            logger.debug("instantiating scoped bean: {}", scopedId);
            Object object = objectFactory.getObject();
            scopedBeans.put(scopedId, object);
        }
        return scopedBeans.get(scopedId);
    }

    @Override
    public Object remove(String name) {
        String scopedId = scopedBeanId(name);
        logger.debug("removing scoped bean from scope: {}", scopedId);
        return scopedBeans.remove(scopedId);
    }

    @Override
    public void registerDestructionCallback(String name, Runnable callback) {
        // noop
    }

    @Override
    public Object resolveContextualObject(String key) {
        return null;
    }
}
