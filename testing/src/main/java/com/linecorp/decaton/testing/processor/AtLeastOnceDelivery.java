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
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.linecorp.decaton.processor.TaskMetadata;

public class AtLeastOnceDelivery implements ProcessingGuarantee {
    private final Set<String> producedIds = Collections.synchronizedSet(new HashSet<>());
    private final Set<String> processedIds = Collections.synchronizedSet(new HashSet<>());

    @Override
    public void onProduce(ProducedRecord record) {
        producedIds.add(record.task().getId());
    }

    @Override
    public void onProcess(TaskMetadata metadata, ProcessedRecord record) {
        processedIds.add(record.task().getId());
    }

    @Override
    public void doAssert() {
        assertEquals(producedIds.size(), processedIds.size());
        //noinspection SimplifiableJUnitAssertion
        assertTrue(producedIds.equals(processedIds));
    }
}
