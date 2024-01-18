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

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.linecorp.decaton.processor.runtime.ProcessorProperties;
import com.linecorp.decaton.processor.runtime.SubPartitioner;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ThreadPoolSubPartitions extends AbstractSubPartitions {
    private final SubPartitioner subPartitioner;
    private final ProcessorUnit[] units;

    public ThreadPoolSubPartitions(PartitionScope scope, Processors<?> processors) {
        super(scope, processors);
        // Create units with latest property value.
        //
        // NOTE: If the property value is changed multiple times at short intervals,
        // each partition processor can have different number of units temporarily.
        // But it's not a problem because all partitions will be kept paused until all reload requests done.
        // Let's see this by example:
        //   1. change concurrency from 1 to 5 => start reloading
        //   2. processor 1,2,3 are reloaded with 5
        //   3. change concurrency from 5 to 3 during reloading => request reloading again, so partitions will be kept paused
        //   4. at next subscription loop, all processors are reloaded with 3 again, then start processing
        int concurrency = scope.props().get(ProcessorProperties.CONFIG_PARTITION_CONCURRENCY).value();
        subPartitioner = scope.subPartitionerSupplier().get(concurrency);
        units = new ProcessorUnit[concurrency];
    }

    @Override
    public void addTask(TaskRequest request) {
        int threadId = subPartitioner.subPartitionFor(request.key());
        if (units[threadId] == null) {
            ExecutorService executor = Executors.newSingleThreadExecutor(
                    Utils.namedThreadFactory("PartitionProcessorThread-" + scope));
            units[threadId] = createUnit(threadId, executor);
        }
        units[threadId].putTask(request);
    }

    @Override
    public void cleanup() {
        // noop
    }

    @Override
    public CompletableFuture<Void> asyncClose() {
        return asyncClose(closeProcessorUnitsAsync(Arrays.stream(units)));
    }
}
