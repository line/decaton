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

package com.linecorp.decaton.testing.processor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.linecorp.decaton.processor.TaskMetadata;
import com.linecorp.decaton.processor.internal.HashableByteArray;

import lombok.Value;

public class ProcessOrdering implements ProcessingGuarantee {
    private final ConcurrentMap<TestTask, Long> taskToOffset = new ConcurrentHashMap<>();

    @Value
    static class Key {
        String subscriptionId;
        HashableByteArray recordKey;
    }

    private final ConcurrentMap<HashableByteArray, List<TestTask>> producedRecords = new ConcurrentHashMap<>();
    private final ConcurrentMap<HashableByteArray, List<ProcessedRecord>> processedRecords = new ConcurrentHashMap<>();

    @Override
    public void onProduce(ProducedRecord record) {
        taskToOffset.put(record.task(), record.offset());
        producedRecords.computeIfAbsent(new HashableByteArray(record.key()),
                                        key -> new ArrayList<>()).add(record.task());
    }

    @Override
    public void onProcess(TaskMetadata metadata, ProcessedRecord record) {
        processedRecords.computeIfAbsent(new HashableByteArray(record.key()),
                                         key -> new ArrayList<>()).add(record);
    }

    @Override
    public void doAssert() {
        for (Entry<HashableByteArray, List<TestTask>> entry : producedRecords.entrySet()) {
            final HashableByteArray recordKey = entry.getKey();
            List<TestTask> produced = entry.getValue();
            List<ProcessedRecord> processed = processedRecords.get(recordKey);

            assertNotNull(processed);
            assertOrdering(taskToOffset, produced, processed);
        }
    }

    static void assertOrdering(Map<TestTask, Long> taskToOffset,
                               List<TestTask> produced,
                               List<ProcessedRecord> processed) {
        System.err.println("produced = " + produced.size() + ", processed = " + processed.size());
        Deque<TestTask> excludeReprocess = new ArrayDeque<>();

        ProcessedRecord headTask = processed.get(0);
        excludeReprocess.addLast(headTask.task());
        long currentOffset = taskToOffset.get(headTask.task());

        long committed = -1L;
        for (int i = 1; i < processed.size(); i++) {
            ProcessedRecord task = processed.get(i);
            long offset = taskToOffset.get(task.task());

            if (offset < committed) {
                fail("offset cannot be regressed beyond committed offset");
            }
            if (offset <= currentOffset) {
                // offset regression implies reprocessing from committed offset happened
                committed = offset;
                System.err.println("Regress to " + committed);

                // rewind records to committed offset
                TestTask last;
                while ((last = excludeReprocess.peekLast()) != null) {
                    if (taskToOffset.get(last) == offset) {
                        break;
                    }
                    excludeReprocess.removeLast();
                }
            } else {
                excludeReprocess.add(task.task());
            }
            currentOffset = offset;
        }

        if (produced.size() != excludeReprocess.size()) {
            processed.forEach(t -> {
                Long offset = taskToOffset.get(t.task());
                System.err.printf("%d %s\n", offset, t.subscriptionId());
            });
        }
        assertEquals(produced.size(), excludeReprocess.size());
        //noinspection SimplifiableJUnitAssertion
        assertTrue(produced.equals(new ArrayList<>(excludeReprocess)));
    }
}
