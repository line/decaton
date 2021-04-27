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

package com.linecorp.decaton.testing;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Time;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.CoreUtils;
import kafka.utils.TestUtils;
import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import scala.Option;

/**
 * Starts embedded Kafka brokers in a same process on random ports
 */
@Slf4j
public class EmbeddedKafkaCluster implements AutoCloseable {
    /**
     * Kafka errors if we attempt to delete topics when running on windows;
     * see https://issues.apache.org/jira/browse/KAFKA-1194
     */
    public static final boolean IS_WINDOWS = System.getProperty("os.name").startsWith("Windows");
    private final List<KafkaServer> servers;
    @Getter
    @Accessors(fluent = true)
    private final String bootstrapServers;

    public EmbeddedKafkaCluster(int numBrokers, String zkConnect) {
        this(numBrokers, zkConnect, new Properties());
    }

    public EmbeddedKafkaCluster(int numBrokers,
                                String zkConnect,
                                Properties brokerProperties) {
        servers = new ArrayList<>(numBrokers);
        List<String> listeners = new ArrayList<>(numBrokers);

        for (int i = 0; i < numBrokers; i++) {
            Properties prop = createBrokerConfig(i, zkConnect);
            prop.putAll(brokerProperties);
            KafkaServer server = TestUtils.createServer(KafkaConfig.fromProps(prop), Time.SYSTEM);
            int port = TestUtils.boundPort(server, SecurityProtocol.PLAINTEXT);
            String listener = "127.0.0.1:" + port;
            listeners.add(listener);
            servers.add(server);

            log.info("Broker {} started at {}", i, listener);
        }

        bootstrapServers = String.join(",", listeners);
    }

    private static Properties createBrokerConfig(int brokerId, String zkConnect) {
        return TestUtils.createBrokerConfig(brokerId, zkConnect,
                                            false, // disable controlled shutdown
                                            !IS_WINDOWS, // enable delete topic except on Windows
                                            0, // use random port

                                            // << enable only PLAINTEXT
                                            Option.empty(),
                                            Option.empty(), Option.empty(),
                                            true, false, 0,
                                            false, 0, false, 0,
                                            // enable only PLAINTEXT >>

                                            Option.empty(), // omit rack information
                                            1, // logDir count
                                            false, // disable delegation token
                                            1, // num partitions
                                            (short) 1 // default replication factor
        );
    }

    @Override
    public void close() {
        for (KafkaServer server : servers) {
            try {
                server.shutdown();
                server.awaitShutdown();
            } catch (Exception e) {
                log.warn("Kafka broker {} threw an exception during shutting down", server.config().brokerId(), e);
            }

            try {
                CoreUtils.delete(server.config().logDirs());
            } catch (Exception e) {
                log.warn("Failed to delete log dirs {}", server.config().logDirs(), e);
            }
        }
    }
}
