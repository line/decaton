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

import java.util.HashMap;
import java.util.Map;

public class ReprocessAwareOrdering implements ProcessingGuarantee {
    @Override
    public void onProduce(ProducedRecord record) {
        // TBD
    }

    @Override
    public void onProcess(ProcessedRecord record) {
        // TBD
    }

    @Override
    public void doAssert() {
//        // trivial checking
//        if (produced.isEmpty() && processed.isEmpty()) {
//            return true;
//        }
//        if (produced.isEmpty()) {
//            return false;
//        }
//        if (produced.size() > processed.size()) {
//            return false;
//        }
//        Map<T, Integer> toIndex = new HashMap<>();
//        for (int i = 0; i < produced.size(); i++) {
//            toIndex.putIfAbsent(produced.get(i), i);
//        }
//
//        // offset in produced tasks which is currently being processed
//        // ("offset" here means "sequence in subpartition" rather than Kafka's offset)
//        int offset = -1;
//        for (int i = 0; i < processed.size(); i++) {
//            // if there is a room, advance the offset
//            if (offset < produced.size() - 1) {
//                offset++;
//            }
//
//            T x = produced.get(offset);
//            T y = processed.get(i);
//            if (!x.equals(y)) {
//                // if processing task doesn't match to produced task, it means re-consuming happens.
//                // lookup the offset to rewind
//                Integer lookup = toIndex.get(y);
//
//                // rewound offset cannot be greater than current offset
//                if (lookup == null || lookup > offset) {
//                    return false;
//                }
//                offset = lookup;
//            }
//        }
//        // must be processed until the end of produced tasks
//        return offset == produced.size() - 1;
    }
}
