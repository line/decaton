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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class SerialProcessing implements ProcessingGuarantee {
    private final List<ProcessedRecord> records =
            Collections.synchronizedList(new ArrayList<>());

    @Override
    public void onProduce(ProducedRecord record) {
        // noop
    }

    @Override
    public void onProcess(ProcessedRecord record) {
        records.add(record);
    }

    @Override
    public void doAssert() {
        Map<String, List<ProcessedRecord>> recordsMap = new HashMap<>();

        for (ProcessedRecord record : records) {
            recordsMap.computeIfAbsent(record.key(),
                                       key -> new ArrayList<>()).add(record);
        }

        // Checks there's no overlap between two consecutive records' processing time
        for (Entry<String, List<ProcessedRecord>> entry : recordsMap.entrySet()) {
            List<ProcessedRecord> perKeyRecords = entry.getValue();
            perKeyRecords.sort(Comparator.comparingLong(ProcessedRecord::startTimeNanos));

            for (int i = 1; i < perKeyRecords.size(); i++) {
                ProcessedRecord prev = perKeyRecords.get(i - 1);
                ProcessedRecord current = perKeyRecords.get(i);

                assertThat("Process time shouldn't overlap. key: " + entry.getKey(),
                           prev.completeTimeNanos(), lessThan(current.startTimeNanos()));
            }
        }
    }
}
