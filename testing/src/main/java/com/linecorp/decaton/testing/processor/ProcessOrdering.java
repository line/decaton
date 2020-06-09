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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class ProcessOrdering implements ProcessingGuarantee {
    private final List<ProducedRecord> producedRecords =
            Collections.synchronizedList(new ArrayList<>());
    private final List<ProcessedRecord> processedRecords =
            Collections.synchronizedList(new ArrayList<>());

    @Override
    public void onProduce(ProducedRecord record) {
        producedRecords.add(record);
    }

    @Override
    public void onProcess(ProcessedRecord record) {
        processedRecords.add(record);
    }

    @Override
    public void doAssert() {
        Map<String, List<TestTask>> perKeyProducedRecords = new HashMap<>();
        Map<String, List<TestTask>> perKeyProcessedRecords = new HashMap<>();
        Map<TestTask, Long> taskToOffset = new HashMap<>();

        for (ProducedRecord record : producedRecords) {
            taskToOffset.put(record.task(), record.offset());
            perKeyProducedRecords.computeIfAbsent(record.key(),
                                                  key -> new ArrayList<>()).add(record.task());
        }

        for (ProcessedRecord record : processedRecords) {
            perKeyProcessedRecords.computeIfAbsent(record.key(),
                                                   key -> new ArrayList<>()).add(record.task());
        }

        for (Entry<String, List<TestTask>> entry : perKeyProducedRecords.entrySet()) {
            String key = entry.getKey();
            List<TestTask> produced = entry.getValue();
            List<TestTask> processed = perKeyProcessedRecords.get(key);

            assertNotNull(processed);
            assertOrdering(taskToOffset, produced, processed);
        }
    }

    static void assertOrdering(Map<TestTask, Long> taskToOffset,
                               List<TestTask> produced,
                               List<TestTask> processed) {
        Deque<TestTask> excludeReprocess = new ArrayDeque<>();

        TestTask headTask = processed.get(0);
        excludeReprocess.addLast(headTask);
        long currentOffset = taskToOffset.get(headTask);

        long committed = -1L;
        for (int i = 1; i < processed.size(); i++) {
            TestTask task = processed.get(i);
            long offset = taskToOffset.get(task);

            if (offset < committed) {
                fail("offset cannot be regressed beyond committed offset");
            }
            if (offset <= currentOffset) {
                // offset regression implies reprocessing from committed offset happened
                committed = offset;

                // rewind records to committed offset
                TestTask last;
                while ((last = excludeReprocess.peekLast()) != null) {
                    if (taskToOffset.get(last) == offset) {
                        break;
                    }
                    excludeReprocess.removeLast();
                }
            } else {
                excludeReprocess.add(task);
            }
            currentOffset = offset;
        }

        assertEquals(produced.size(), excludeReprocess.size());
        //noinspection SimplifiableJUnitAssertion
        assertTrue(produced.equals(new ArrayList<>(excludeReprocess)));
    }
}
