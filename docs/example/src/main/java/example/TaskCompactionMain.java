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
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.linecorp.decaton.processor.TaskMetadata;
import com.linecorp.decaton.processor.processors.CompactionProcessor;
import com.linecorp.decaton.processor.processors.CompactionProcessor.CompactChoice;
import com.linecorp.decaton.processor.runtime.DecatonTask;
import com.linecorp.decaton.processor.runtime.ProcessorScope;
import com.linecorp.decaton.processor.runtime.ProcessorSubscription;
import com.linecorp.decaton.processor.runtime.ProcessorsBuilder;
import com.linecorp.decaton.processor.runtime.SubscriptionBuilder;

import com.linecorp.decaton.processor.runtime.TaskExtractor;
import example.models.LocationEvent;
import example.processors.LocationEventProcessor;

public class TaskCompactionMain {
    public static void main(String[] args) throws Exception {
        Properties consumerConfig = new Properties();
        consumerConfig.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "my-decaton-processor");
        consumerConfig.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                                   System.getProperty("bootstrap.servers"));
        consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-decaton-processor");

        TaskExtractor<LocationEvent> extractor = bytes -> {
            TaskMetadata metadata = TaskMetadata.builder().build();
            LocationEvent data;
            try {
                data = new ObjectMapper().readValue(bytes, LocationEvent.class);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            return new DecatonTask<>(metadata, data, bytes);
        };

        ProcessorSubscription subscription =
                SubscriptionBuilder.newBuilder("my-decaton-processor")
                                   .processorsBuilder(
                                           ProcessorsBuilder
                                                   .consuming("my-decaton-topic", extractor)
                                                   .thenProcess(TaskCompactionMain::createCompactionProcessor,
                                                                ProcessorScope.THREAD)
                                                   .thenProcess(LocationEventProcessor::new,
                                                                ProcessorScope.THREAD)
                                   )
                                   .consumerConfig(consumerConfig)
                                   .buildAndStart();

        Thread.sleep(10000);
        subscription.close();
    }

    private static CompactionProcessor<LocationEvent> createCompactionProcessor() {
        return new CompactionProcessor<>(1000L, (left, right) -> {
            if (left.task().getTimestamp() == right.task().getTimestamp()) {
                return CompactChoice.PICK_EITHER;
            } else if (left.task().getTimestamp() > right.task().getTimestamp()) {
                return CompactChoice.PICK_LEFT;
            } else {
                return CompactChoice.PICK_RIGHT;
            }
        });
    }
}
