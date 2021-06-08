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

package example;

import java.io.IOException;
import java.io.UncheckedIOException;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.linecorp.decaton.processor.TaskMetadata;

import com.linecorp.decaton.processor.runtime.DecatonTask;
import com.linecorp.decaton.processor.runtime.TaskExtractor;
import example.models.UserEvent;

public class JSONUserEventExtractor implements TaskExtractor<UserEvent> {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public DecatonTask<UserEvent> extract(byte[] bytes) {
        try {
            UserEvent event = MAPPER.readValue(bytes, UserEvent.class);
            TaskMetadata metadata = TaskMetadata.builder()
                                                // Filling timestampMillis is not mandatory, but it would be useful
                                                // when you monitor delivery latency between event production time and event processing time.
                                                .timestampMillis(event.getEventTimestampMillis())
                                                // This field is not mandatory too, but you can track which application produced the task by filling this.
                                                .sourceApplicationId("event-tracker")
                                                // You can set other TaskMetadata fields as you needed
                                                .build();

            return new DecatonTask<>(metadata, event, bytes);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
