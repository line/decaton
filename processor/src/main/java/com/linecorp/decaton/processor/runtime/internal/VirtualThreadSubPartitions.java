/*
 * Copyright 2024 LY Corporation
 *
 * LY Corporation licenses this file to you under the Apache License,
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
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

    public VirtualThreadSubPartitions(PartitionScope scope, Processors<?> processors) {
        super(scope, processors);
        subPartitioner = scope.subPartitionerSupplier().get(Integer.MAX_VALUE);
        units = new HashMap<>();
    }

    @Override
    public void addTask(TaskRequest request) {
        int threadId = subPartitioner.subPartitionFor(request.key());
        units.computeIfAbsent(threadId, key -> {
            ExecutorService executor = Executors.newSingleThreadExecutor(
                    Utils.namedVirtualThreadFactory("PartitionProcessorVThread-" + scope));
            return createUnit(new ThreadScope(scope, threadId), executor);
        }).putTask(request);
    }

    @Override
    public void cleanup() {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        Iterator<Entry<Integer, ProcessorUnit>> iter = units.entrySet().iterator();
        while (iter.hasNext()) {
            Entry<Integer, ProcessorUnit> entry = iter.next();
            int threadId = entry.getKey();
            ProcessorUnit unit = entry.getValue();
            if (!unit.hasPendingTasks()) {
                try {
                    iter.remove();
                    CompletableFuture<Void> future =
                            unit.asyncClose().whenComplete((ignored, e) -> destroyThreadProcessor(unit.id()));
                    futures.add(future);
                } catch (Exception e) {
                    log.warn("Failed to close processor unit of {}", threadId, e);
                }
            }
        }
        futures.forEach(CompletableFuture::join);
    }

    @Override
    public CompletableFuture<Void> asyncClose() {
        return asyncClose(CompletableFuture.allOf(
                units.values().stream().map(this::closeUnitAsync)
                     .toArray(CompletableFuture[]::new)));
    }
}
