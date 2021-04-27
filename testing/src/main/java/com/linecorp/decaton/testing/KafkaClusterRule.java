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

import java.util.Properties;

import org.junit.Rule;
import org.junit.rules.ExternalResource;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

/**
 * JUnit {@link Rule} that starts an embedded Kafka cluster.
 */
@Slf4j
@RequiredArgsConstructor
public class KafkaClusterRule extends ExternalResource {
    private static final int KAFKA_CLUSTER_SIZE = 3;

    private EmbeddedZooKeeper zooKeeper;
    private EmbeddedKafkaCluster kafkaCluster;
    @Getter
    @Accessors(fluent = true)
    private KafkaAdmin admin;
    private final Properties brokerProperties;

    public KafkaClusterRule() {
        this(new Properties());
    }

    public String bootstrapServers() {
        return kafkaCluster.bootstrapServers();
    }

    @Override
    protected void before() throws Throwable {
        super.before();

        zooKeeper = new EmbeddedZooKeeper();
        kafkaCluster = new EmbeddedKafkaCluster(KAFKA_CLUSTER_SIZE,
                                                zooKeeper.zkConnectAsString(),
                                                brokerProperties);
        admin = new KafkaAdmin(kafkaCluster.bootstrapServers());
    }

    @Override
    protected void after() {
        safeClose(admin);
        safeClose(kafkaCluster);
        safeClose(zooKeeper);
        super.after();
    }

    private static void safeClose(AutoCloseable resource) {
        try {
            resource.close();
        } catch (Exception e) {
            log.warn("Failed to close the resource", e);
        }
    }
}
