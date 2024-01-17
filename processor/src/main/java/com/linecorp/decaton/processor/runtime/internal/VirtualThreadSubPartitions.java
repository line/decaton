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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.linecorp.decaton.processor.runtime.SubPartitioner;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class VirtualThreadSubPartitions extends AbstractSubPartitions {
    private final SubPartitioner subPartitioner;
    private final Map<Integer, ProcessorUnit> units;

    public VirtualThreadSubPartitions(PartitionScope scope,  Processors<?> processors) {
        super(scope, processors);
        subPartitioner = scope.subPartitionerSupplier().get(Integer.MAX_VALUE);
        units = new HashMap<>();
    }

    @Override
    public void addTask(TaskRequest request) {
        int threadId = subPartitioner.subPartitionFor(request.key());
        units.computeIfAbsent(threadId, key -> {
            // TODO: should we use PerTask + lock or single thread
//        ExecutorService executor = Executors.newThreadPerTaskExecutor(
//                Utils.namedVirtualThreadFactory("PartitionProcessorVThread-" + scope));
            ExecutorService executor = Executors.newSingleThreadExecutor(
                    Utils.namedVirtualThreadFactory("PartitionProcessorVThread-" + scope));
            return createUnit(threadId, executor);
        }).putTask(request);
    }

    @Override
    public void cleanup() {
        for (Entry<Integer, ProcessorUnit> entry : new HashSet<>(units.entrySet())) {
            int threadId = entry.getKey();
            ProcessorUnit unit = entry.getValue();
            if (!unit.hasPendingTasks()) {
                try {
                    units.remove(threadId).close(); // TODO: should we use async API instead?
                    destroyThreadProcessor(unit.id());
                } catch (Exception e) {
                    log.warn("Failed to close processor unit of {}", threadId, e);
                }
            }
        }
    }

    @Override
    public CompletableFuture<Void> asyncClose() {
        return asyncClose(closeProcessorUnitsAsync(units.values().stream()));
    }
}
