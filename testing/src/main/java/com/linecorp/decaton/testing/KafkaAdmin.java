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

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import lombok.extern.slf4j.Slf4j;

/**
 * A wrapper of {@link AdminClient} for providing easy operation to manage topics
 */
@Slf4j
public class KafkaAdmin implements AutoCloseable {
    private final AdminClient adminClient;

    public KafkaAdmin(String bootstrapServers) {
        Properties props = new Properties();
        props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.setProperty(AdminClientConfig.CLIENT_ID_CONFIG, "admin");

        adminClient = AdminClient.create(props);
    }

    /**
     * Create a topic with random name
     * @return created topic name
     */
    public String createRandomTopic(int numPartitions, int replicationFactor) {
        String topicName = "test-" + UUID.randomUUID();
        createTopic(topicName, numPartitions, replicationFactor);
        return topicName;
    }

    public void createTopic(String topicName, int numPartitions, int replicationFactor) {
        NewTopic newTopic = new NewTopic(topicName, numPartitions, (short) replicationFactor);

        try {
            adminClient.createTopics(Collections.singleton(newTopic)).all().get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void deleteTopics(boolean ignoreFailures, String... topicNames) {
        try {
            adminClient.deleteTopics(Arrays.asList(topicNames)).all().get();
        } catch (Exception e) {
            if (ignoreFailures) {
                log.info("Failure while deleting topics {}, ignoring", topicNames, e);
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    public Map<TopicPartition, OffsetAndMetadata> consumerGroupOffsets(String groupId) {
        try {
            return adminClient.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        adminClient.close();
    }
}
