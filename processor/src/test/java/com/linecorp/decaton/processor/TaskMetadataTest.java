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

package com.linecorp.decaton.processor;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class TaskMetadataTest {
    @Test
    public void testDefaultValue() {
        TaskMetadata metadata = TaskMetadata.builder().build();
        TaskMetadata defaultMeta = new TaskMetadata(0L, "", "", 0L, 0L);

        assertEquals(defaultMeta, metadata);
    }

    @Test
    public void testCompleteness() {
        TaskMetadata original = new TaskMetadata(1L, "app", "instance", 2L, 3L);

        assertEquals(original, TaskMetadata.fromProto(original.toProto()));
    }
}
