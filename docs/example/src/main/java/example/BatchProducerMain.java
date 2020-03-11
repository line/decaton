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

import org.apache.kafka.clients.producer.ProducerConfig;

import com.linecorp.decaton.client.DecatonClient;
import com.linecorp.decaton.example.protocol.Mytasks.PrintMessageTask;
import com.linecorp.decaton.protobuf.ProtocolBuffersSerializer;

public final class BatchProducerMain {
    public static void main(String[] args) throws Exception {
        try (DecatonClient<PrintMessageTask> client = newClient()) {
            for (int i = 0; i < 100; i++) {
                String name = "name:" + i;
                PrintMessageTask task = PrintMessageTask.newBuilder().setName(name).setAge(i).build();
                client.put(name, task)
                      .whenComplete((r, e) -> {
                          if (e != null) {
                              System.err.println("Producing task failed... " + e);
                          }
                      });
            }
        }
    }

    private static DecatonClient<PrintMessageTask> newClient() {
        Properties producerConfig = new Properties();
        producerConfig.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "my-decaton-client");
        producerConfig.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                                   System.getProperty("bootstrap.servers"));

        return DecatonClient.producing("my-decaton-topic",
                                       new ProtocolBuffersSerializer<PrintMessageTask>())
                            .applicationId("ProducerMain")
                            // By default it sets local hostname but here we go explicit
                            .instanceId("localhost")
                            .producerConfig(producerConfig)
                            .build();
    }
}
