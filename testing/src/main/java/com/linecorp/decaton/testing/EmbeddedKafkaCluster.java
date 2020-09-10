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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Time;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.CoreUtils;
import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import scala.Option;

/**
 * Starts embedded Kafka brokers in a same process on random ports
 */
@Slf4j
public class EmbeddedKafkaCluster implements AutoCloseable {
    private final List<KafkaServer> servers;
    @Getter
    @Accessors(fluent = true)
    private final String bootstrapServers;

    public EmbeddedKafkaCluster(int numBrokers, String zkConnect) {
        servers = new ArrayList<>(numBrokers);
        List<String> listeners = new ArrayList<>(numBrokers);

        for (int i = 0; i < numBrokers; i++) {
            Properties prop = createBrokerConfig(i, zkConnect);

            KafkaServer server = new KafkaServer(KafkaConfig.fromProps(prop),
                                                 Time.SYSTEM,
                                                 Option.empty(),
                                                 scala.collection.immutable.List.empty());
            server.startup();
            int port = server.boundPort(ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT));
            String listener = "127.0.0.1:" + port;
            listeners.add(listener);
            servers.add(server);

            log.info("Broker {} started at {}", i, listener);
        }

        bootstrapServers = String.join(",", listeners);
    }

    private static Properties createBrokerConfig(int brokerId, String zkConnect) {
        Properties prop = new Properties();

        prop.setProperty("broker.id", String.valueOf(brokerId));
        prop.setProperty("zookeeper.connect", zkConnect);
        prop.setProperty("controlled.shutdown.enable", "false");
        prop.setProperty("delete.topic.enable", "true");
        prop.setProperty("listeners", "PLAINTEXT://localhost:0");
        try {
            prop.setProperty("log.dir",
                             Files.createTempDirectory("zookeeper-logs").toFile().getAbsolutePath());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        prop.setProperty("num.partitions", "1");
        prop.setProperty("default.replication.factor", "3");
        prop.setProperty("zookeeper.connection.timeout.ms", "10000");
        prop.setProperty("replica.socket.timeout.ms", "1500");
        prop.setProperty("controller.socket.timeout.ms", "1500");
        prop.setProperty("log.segment.delete.delay.ms", "1000");
        prop.setProperty("log.cleaner.dedupe.buffer.size", "2097152");
        prop.setProperty("message.timestamp.difference.max.ms", String.valueOf(Long.MAX_VALUE));
        prop.setProperty("offsets.topic.replication.factor", "1");
        prop.setProperty("offsets.topic.num.partitions", "5");
        prop.setProperty("group.initial.rebalance.delay.ms", "0");

        return prop;
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
