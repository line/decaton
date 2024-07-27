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
import com.linecorp.decaton.processor.processors.BatchingProcessor;
import com.linecorp.decaton.processor.runtime.DecatonTask;
import com.linecorp.decaton.processor.runtime.ProcessorScope;
import com.linecorp.decaton.processor.runtime.ProcessorSubscription;
import com.linecorp.decaton.processor.runtime.ProcessorsBuilder;
import com.linecorp.decaton.processor.runtime.SubscriptionBuilder;
import com.linecorp.decaton.processor.runtime.TaskExtractor;
import com.linecorp.decaton.protocol.Sample.HelloTask;

import example.processors.InsertHelloTaskBatchingProcessor;

public class TaskBatchingMain {
    public static void main(String[] args) throws Exception {
        Properties consumerConfig = new Properties();
        consumerConfig.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "my-decaton-processor");
        String bootstrapServers = System.getProperty("bootstrap.servers", "localhost:9092");
        consumerConfig.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-decaton-processor");

        TaskExtractor<HelloTask> extractor = bytes -> {
            TaskMetadata metadata = TaskMetadata.builder().build();
            HelloTask data;
            try {
                data = new ObjectMapper().readValue(bytes, HelloTask.class);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            return new DecatonTask<>(metadata, data, bytes);
        };
        long lingerMillis = 1000;
        int capacity = 100;
        ProcessorSubscription subscription =
                SubscriptionBuilder.newBuilder("my-decaton-processor")
                                   .processorsBuilder(
                                           ProcessorsBuilder
                                                   .consuming("my-decaton-topic", extractor)
                                                   .thenProcess(() -> createBatchingProcessor(lingerMillis,
                                                                                              capacity),
                                                                ProcessorScope.THREAD)
                                   )
                                   .consumerConfig(consumerConfig)
                                   .buildAndStart();

        Thread.sleep(10000);
        subscription.close();
    }

    private static BatchingProcessor<HelloTask> createBatchingProcessor(long lingerMillis, int capacity) {
        return new InsertHelloTaskBatchingProcessor(lingerMillis, capacity); // <1>
    }
}
