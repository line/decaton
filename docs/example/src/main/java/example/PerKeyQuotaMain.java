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

import static com.linecorp.decaton.processor.runtime.ProcessorProperties.CONFIG_PER_KEY_QUOTA_RATE;
import static com.linecorp.decaton.processor.runtime.ProcessorProperties.CONFIG_PROCESSING_RATE;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import com.linecorp.decaton.example.protocol.Mytasks.PrintMessageTask;
import com.linecorp.decaton.processor.runtime.PerKeyQuotaConfig;
import com.linecorp.decaton.processor.runtime.PerKeyQuotaConfig.QuotaCallback.Action;
import com.linecorp.decaton.processor.runtime.ProcessorSubscription;
import com.linecorp.decaton.processor.runtime.ProcessorsBuilder;
import com.linecorp.decaton.processor.runtime.Property;
import com.linecorp.decaton.processor.runtime.StaticPropertySupplier;
import com.linecorp.decaton.processor.runtime.SubscriptionBuilder;
import com.linecorp.decaton.protobuf.ProtocolBuffersDeserializer;

import example.processors.PrintMessageTaskProcessor;

public class PerKeyQuotaMain {
    public static void main(String[] args) throws Exception {

        Properties consumerConfig = new Properties();
        consumerConfig.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "my-decaton-processor");
        consumerConfig.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                                   System.getProperty("bootstrap.servers"));
        consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-decaton-processor");

        ProcessorSubscription shapingSubscription =
                SubscriptionBuilder.newBuilder("shaping-processor")
                                   .properties(StaticPropertySupplier.of(Property.ofStatic(CONFIG_PER_KEY_QUOTA_RATE, 100L)))
                                   .enablePerKeyQuota(PerKeyQuotaConfig.shape())
                                   .processorsBuilder(
                                           ProcessorsBuilder.consuming(
                                                   "my-decaton-topic",
                                                   new ProtocolBuffersDeserializer<>(PrintMessageTask.parser()))
                                                            .thenProcess(new PrintMessageTaskProcessor()))
                                   .consumerConfig(consumerConfig)
                                   .buildAndStart();
        ProcessorSubscription dropSubscription =
                SubscriptionBuilder.newBuilder("drop-processor")
                                   .properties(StaticPropertySupplier.of(Property.ofStatic(CONFIG_PER_KEY_QUOTA_RATE, 100L)))
                                   .enablePerKeyQuota(PerKeyQuotaConfig.drop())
                                   .processorsBuilder(
                                           ProcessorsBuilder.consuming(
                                                                    "my-decaton-topic",
                                                                    new ProtocolBuffersDeserializer<>(PrintMessageTask.parser()))
                                                            .thenProcess(new PrintMessageTaskProcessor()))
                                   .consumerConfig(consumerConfig)
                                   .buildAndStart();
        ProcessorSubscription customSubscription =
                SubscriptionBuilder.newBuilder("custom-quota-processor")
                                   .properties(StaticPropertySupplier.of(
                                           Property.ofStatic(CONFIG_PER_KEY_QUOTA_RATE, 100L),
                                           Property.ofStatic(PerKeyQuotaConfig.topicProcessingRateProperty("my-decaton-topic-shaping-x"), 80L),
                                           Property.ofStatic(PerKeyQuotaConfig.topicProcessingRateProperty("my-decaton-topic-shaping-y"), 50L)))
                                   .enablePerKeyQuota(PerKeyQuotaConfig
                                                              .builder()
                                                              .shapingTopics("my-decaton-topic-shaping-x", "my-decaton-topic-shaping-y")
                                                              .<PrintMessageTask>callback((key, metadata, task, observation) -> {
                                                                  if (task.getName().contains("x")) {
                                                                      return Action.shape("my-decaton-topic-shaping-x");
                                                                  }
                                                                  if (task.getName().contains("y")) {
                                                                      return Action.shape("my-decaton-topic-shaping-y");
                                                                  }
                                                                  return Action.drop();
                                                              })
                                                              .build())
                                   .processorsBuilder(
                                           ProcessorsBuilder.consuming(
                                                                    "my-decaton-topic",
                                                                    new ProtocolBuffersDeserializer<>(PrintMessageTask.parser()))
                                                            .thenProcess(new PrintMessageTaskProcessor()))
                                   .consumerConfig(consumerConfig)
                                   .buildAndStart();

        Thread.sleep(10000);
        shapingSubscription.close();
        dropSubscription.close();
        customSubscription.close();
    }
}
