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

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import com.linecorp.centraldogma.client.CentralDogma;
import com.linecorp.centraldogma.client.armeria.ArmeriaCentralDogmaBuilder;
import com.linecorp.decaton.centraldogma.CentralDogmaPropertySupplier;
import com.linecorp.decaton.example.protocol.Mytasks.PrintMessageTask;
import com.linecorp.decaton.processor.runtime.ProcessorSubscription;
import com.linecorp.decaton.processor.runtime.ProcessorsBuilder;
import com.linecorp.decaton.processor.runtime.SubscriptionBuilder;
import com.linecorp.decaton.protobuf.ProtocolBuffersDeserializer;

import example.processors.PrintMessageTaskProcessor;

public class CentralDogmaSupplierMain {
    public static void main(String[] args) throws Exception {
        final CentralDogma centralDogma = new ArmeriaCentralDogmaBuilder()
                .host("127.0.0.1")
                .accessToken("accesstoken")
                .build();

        CentralDogmaPropertySupplier supplier = CentralDogmaPropertySupplier
                .register(centralDogma, "project", "repository", "/testProcessor.json");

        Properties consumerConfig = new Properties();
        consumerConfig.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "my-decaton-processor");
        consumerConfig.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                                   System.getProperty("bootstrap.servers"));
        consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-decaton-processor");

        ProcessorSubscription testProcessor =
                SubscriptionBuilder.newBuilder("testProcessor")
                                   .processorsBuilder(
                                           ProcessorsBuilder.consuming(
                                                   "my-decaton-topic",
                                                   new ProtocolBuffersDeserializer<>(PrintMessageTask.parser()))
                                                            .thenProcess(new PrintMessageTaskProcessor())
                                   )
                                   .properties(supplier)
                                   .consumerConfig(consumerConfig)
                                   .buildAndStart();
    }
}
