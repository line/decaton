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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linecorp.decaton.processor.DecatonProcessor;
import com.linecorp.decaton.processor.runtime.DecatonProcessorSupplier;
import com.linecorp.decaton.processor.runtime.ProcessorScope;

public class DecatonProcessorSupplierImpl<T> implements DecatonProcessorSupplier<T> {
    private static final Logger logger = LoggerFactory.getLogger(DecatonProcessorSupplierImpl.class);

    private final Supplier<DecatonProcessor<T>> supplier;
    private final ProcessorScope creationScope;
    private final ConcurrentMap<String, DecatonProcessor<T>> processors;

    public DecatonProcessorSupplierImpl(Supplier<DecatonProcessor<T>> supplier, ProcessorScope creationScope) {
        this.supplier = supplier;
        this.creationScope = creationScope;
        processors = new ConcurrentHashMap<>();
    }

    private String processorKey(TopicPartition tp, Integer threadId) {
        switch (creationScope) {
            case PROVIDED:
            case SINGLETON:
                return creationScope.name();
            case PARTITION:
                return tp.toString();
            case THREAD:
                return tp.toString() + '/' + threadId;
            default:
                throw new IllegalStateException("unknown creation scope: " + creationScope);
        }
    }

    private DecatonProcessor<T> getOrCreateProcessor(String subscriptionId, TopicPartition tp, int threadId) {
        String processorKey = processorKey(tp, threadId);
        return processors.computeIfAbsent(processorKey, key -> {
            logger.debug("Creating processor for '{}'", key);
            return DecatonProcessingContext.withContext(subscriptionId, tp, threadId, supplier);
        });
    }

    private void disposeProcessor(String subscriptionId, TopicPartition tp, Integer threadId) {
        String processorKey = processorKey(tp, threadId);

        DecatonProcessor<T> processor = processors.remove(processorKey);
        if (processor != null && creationScope != ProcessorScope.PROVIDED) {
            DecatonProcessingContext.withContext(subscriptionId, tp, threadId, () -> {
                try {
                    logger.debug("Closing processor for '{}'", processorKey);
                    processor.close();
                } catch (Exception e) {
                    logger.warn("closing processor {} threw exception", processor, e);
                }
                return null;
            });
        }
    }

    @Override
    public DecatonProcessor<T> getProcessor(String subscriptionId, TopicPartition tp, int threadId) {
        return getOrCreateProcessor(subscriptionId, tp, threadId);
    }

    @Override
    public void leaveSingletonScope(String subscriptionId) {
        if (creationScope == ProcessorScope.PROVIDED || creationScope == ProcessorScope.SINGLETON) {
            disposeProcessor(subscriptionId, null, null);
        }
    }

    @Override
    public void leavePartitionScope(String subscriptionId, TopicPartition tp) {
        if (creationScope == ProcessorScope.PARTITION) {
            disposeProcessor(subscriptionId, tp, null);
        }
    }

    @Override
    public void leaveThreadScope(String subscriptionId, TopicPartition tp, int threadId) {
        if (creationScope == ProcessorScope.THREAD) {
            disposeProcessor(subscriptionId, tp, threadId);
        }
    }
}
