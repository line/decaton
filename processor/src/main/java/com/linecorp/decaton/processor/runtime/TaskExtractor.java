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

package com.linecorp.decaton.processor.runtime;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * An interface for classes extracting {@link DecatonTask} from given record.
 * @param <T> type of task.
 */
public interface TaskExtractor<T> {
    /**
     * Extract object of type {@link DecatonTask} from given bytes.
     * @param record {@link ConsumerRecord} to extract task from.
     * @return object of type {@link DecatonTask}.
     * @throws RuntimeException this method can throw arbitrary {@link RuntimeException} if given record is invalid.
     * If the method throws an exception, the task will be discarded and processor continues to process subsequent tasks.
     */
    DecatonTask<T> extract(ConsumerRecord<byte[], byte[]> record);
}
