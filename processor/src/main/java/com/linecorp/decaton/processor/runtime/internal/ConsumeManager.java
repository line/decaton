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

package com.linecorp.decaton.processor.runtime.internal;

import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import com.linecorp.decaton.processor.metrics.Metrics.SubscriptionMetrics;
import com.linecorp.decaton.processor.runtime.internal.Utils.Timer;

import lombok.extern.slf4j.Slf4j;

/**
 * Implements workflow of consuming tasks from Kafka.
 */
@Slf4j
public class ConsumeManager implements AutoCloseable {
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(100);

    /**
     * Interface to class storing partition's consumption state information.
     */
    public interface PartitionStates {
        /**
         * Update all partitions state to latest and make below methods to return the fresh information.
         */
        void updatePartitionsStatus();

        /**
         * Return the list of partitions that needs to enter pause state.
         * @return list of partitions to pause.
         */
        List<TopicPartition> partitionsNeedsPause();

        /**
         * Return the list of partitions that needs to recover from pause state.
         * @return list of partitions to resume.
         */
        List<TopicPartition> partitionsNeedsResume();

        /**
         * Report list of partitions that has been successfully paused.
         * @param partitions partitions that has been paused.
         */
        void partitionsPaused(List<TopicPartition> partitions);

        /**
         * Report list of partitions that has been successfully resumed.
         * @param partitions partitions that has been resumed.
         */
        void partitionsResumed(List<TopicPartition> partitions);
    }

    /**
     * Interface to handler class that implements core logic in reaction to consumer events.
     */
    public interface ConsumerHandler {
        /**
         * Do any preparation for upcoming rebalance.
         */
        void prepareForRebalance(Collection<TopicPartition> revokingPartitions);

        /**
         * Update assignment with given partitions list.
         * @param newAssignment list of partitions that are now actively assigned.
         */
        void updateAssignment(Collection<TopicPartition> newAssignment);

        /**
         * Process a {@link ConsumerRecord} that has been consumed from the topic.
         * @param record a record that has been fetched from the target topic.
         */
        void receive(ConsumerRecord<byte[], byte[]> record);
    }

    private final Consumer<byte[], byte[]> consumer;
    private final PartitionStates states;
    private final ConsumerHandler handler;
    private final SubscriptionMetrics metrics;
    private final AtomicBoolean consumerClosing;

    public ConsumeManager(Consumer<byte[], byte[]> consumer,
                          PartitionStates states,
                          ConsumerHandler handler,
                          SubscriptionMetrics metrics) {
        this.consumer = consumer;
        this.states = states;
        this.handler = handler;
        this.metrics = metrics;
        consumerClosing = new AtomicBoolean();
    }

    /**
     * Initialize consumer to being fetching records from the given topics.
     * @param subscribeTopics list of topics to fetch records from.
     */
    public void init(Collection<String> subscribeTopics) {
        Set<TopicPartition> pausedPartitions = new HashSet<>();
        consumer.subscribe(subscribeTopics, new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                // KafkaConsumer#close has been changed to invoke onPartitionRevoked since Kafka 2.4.0.
                // Since we're doing cleanup procedure on shutdown manually
                // so just immediately return if consumer is already closing
                if (consumerClosing.get()) {
                    return;
                }

                handler.prepareForRebalance(partitions);
                pausedPartitions.addAll(consumer.paused());
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> ignored) {
                handler.updateAssignment(consumer.assignment());

                // Consumer rebalance resets all pause states of assigned partitions even though they
                // haven't moved over from/to different consumer instance.
                // Need to re-call pause with originally paused partitions to bring state back consistent.
                pausedPartitions.retainAll(consumer.assignment());
                try {
                    consumer.pause(pausedPartitions);
                } finally {
                    pausedPartitions.clear();
                }
            }
        });
    }

    /**
     * Run a single {@link Consumer#poll(Duration)} to obtain records from subscribing topics.
     * For each consumed record, {@link ConsumerHandler#receive(ConsumerRecord)} called.
     * After finish processing consumed records, {@link PartitionStates#updatePartitionsStatus()} called.
     * According to the values from {@link PartitionStates#partitionsNeedsPause()} and
     * {@link PartitionStates#partitionsNeedsResume()}, the underlying consumer is instructed to stop/resume
     * fetching records from particular partitions and {@link PartitionStates#partitionsPaused(List)} and/or
     * {@link PartitionStates#partitionsResumed(List)} called when there were some partitions newly
     * paused/resumed.
     */
    public void poll() {
        Timer timer = Utils.timer();
        ConsumerRecords<byte[], byte[]> records = consumer.poll(POLL_TIMEOUT);
        metrics.consumerPollTime.record(timer.duration());

        timer = Utils.timer();
        records.forEach(handler::receive);
        metrics.handleRecordsTime.record(timer.duration());

        states.updatePartitionsStatus();

        timer = Utils.timer();
        pausePartitions(consumer);
        resumePartitions(consumer);
        metrics.handlePausesTime.record(timer.duration());
    }

    private void pausePartitions(Consumer<?, ?> consumer) {
        List<TopicPartition> pausedPartitions = states.partitionsNeedsPause();
        if (pausedPartitions.isEmpty()) {
            return;
        }

        log.debug("Pausing partitions: {}", pausedPartitions);
        consumer.pause(pausedPartitions);
        states.partitionsPaused(pausedPartitions);
    }

    private void resumePartitions(Consumer<?, ?> consumer) {
        List<TopicPartition> resumedPartitions = states.partitionsNeedsResume();
        if (resumedPartitions.isEmpty()) {
            return;
        }

        log.debug("Resuming partitions: {}", resumedPartitions);
        consumer.resume(resumedPartitions);
        states.partitionsResumed(resumedPartitions);
    }

    @Override
    public void close() {
        consumerClosing.set(true);
        consumer.close();
    }
}
