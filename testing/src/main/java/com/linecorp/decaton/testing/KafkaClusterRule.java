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

import org.junit.Rule;
import org.junit.rules.ExternalResource;

/**
 * JUnit {@link Rule} that starts an embedded Kafka cluster.
 */
public class KafkaClusterRule extends ExternalResource {
    private static final int KAFKA_CLUSTER_SIZE = 3;

    private EmbeddedZooKeeper zooKeeper;
    private EmbeddedKafkaCluster kafkaCluster;
    private KafkaAdmin admin;

    public String bootstrapServers() {
        return kafkaCluster.bootstrapServers();
    }

    public KafkaAdmin admin() {
        return admin;
    }

    @Override
    protected void before() throws Throwable {
        super.before();

        zooKeeper = new EmbeddedZooKeeper();
        kafkaCluster = new EmbeddedKafkaCluster(KAFKA_CLUSTER_SIZE,
                                                zooKeeper.zkConnectAsString());
        admin = new KafkaAdmin(kafkaCluster.bootstrapServers());
    }

    @Override
    protected void after() {
        safeClose(kafkaCluster);
        safeClose(zooKeeper);
        safeClose(admin);
        super.after();
    }

    private static void safeClose(AutoCloseable resource) {
        try {
            resource.close();
        } catch (Exception e) {
            System.err.println("Failed to close the resource");
        }
    }
}
