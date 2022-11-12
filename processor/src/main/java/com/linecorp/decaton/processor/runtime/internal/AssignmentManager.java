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

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.apache.kafka.common.TopicPartition;

import com.linecorp.decaton.processor.runtime.internal.Utils.Timer;

import lombok.Value;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AssignmentManager {
    /**
     * Configurations representing initial state of partition contexts to be created.
     */
    @Value
    @Accessors(fluent = true)
    static class AssignmentConfig {
        boolean paused;
    }

    /**
     * Interface to an assignment store that stores assigned partitions and its metadata.
     */
    interface AssignmentStore {
        /**
         * Return set of {@link TopicPartition}s that are currently assigned.
         * Note that this have to include all partitions including revoking ones.
         * @return set of assigned topic-partitions.
         */
        Set<TopicPartition> assignedPartitions();

        /**
         * Mark the partitions as revoking
         * @param partitions target partitions to mark
         */
        void markRevoking(Collection<TopicPartition> partitions);

        /**
         * Set the partitions as not revoking
         * @param partitions target partitions to unmark
         */
        void unmarkRevoking(Collection<TopicPartition> partitions);

        /**
         * Add new topic-partitions with associated configurations.
         * @param partitions newly assigned partitions.
         */
        void addPartitions(Map<TopicPartition, AssignmentConfig> partitions);

        /**
         * Remove previously assigned topic-partitions.
         * @param partitions partitions to remove.
         */
        void removePartition(Collection<TopicPartition> partitions);
    }

    private final AssignmentStore store;

    public AssignmentManager(AssignmentStore store) {
        this.store = store;
    }

    /**
     * Update assignment with new set of partitions.
     * Revoked partitions and newly assigned partitions are computed with previous assignment and removed/added
     * from/to the store.
     * @param newAssignment new set of topic-partitions to assign.
     */
    public void assign(Collection<TopicPartition> newAssignment) {
        Set<TopicPartition> newSet = new HashSet<>(newAssignment);
        Set<TopicPartition> oldSet = store.assignedPartitions();
        List<TopicPartition> removed = computeRemovedPartitions(oldSet, newSet);
        List<TopicPartition> added = computeAddedPartitions(oldSet, newSet);
        log.debug("Assignment update: removed:{}, added:{}, assignment:{}", removed, added, newSet);

        partitionsRevoked(removed);
        partitionsAssigned(added);
        store.unmarkRevoking(newSet);
    }

    /**
     * Repair given topic-partition that has detected as its offset has regression.
     * {@link PartitionContext} associated with the given topic-partition gets re-created.
     * @param tp topic-partition to repair.
     */
    public void repair(TopicPartition tp) {
        log.info("Repairing partition: {}", tp);
        List<TopicPartition> target = Collections.singletonList(tp);
        partitionsRevoked(target);
        partitionsAssigned(target);
    }

    private static List<TopicPartition> computeRemovedPartitions(
            Set<TopicPartition> oldSet, Set<TopicPartition> newSet) {
        return oldSet.stream().filter(tp -> !newSet.contains(tp)).collect(toList());
    }

    private static List<TopicPartition> computeAddedPartitions(
            Set<TopicPartition> oldSet, Set<TopicPartition> newSet) {
        return newSet.stream().filter(tp -> !oldSet.contains(tp)).collect(toList());
    }

    private void partitionsRevoked(Collection<TopicPartition> partitions) {
        if (partitions.isEmpty()) {
            return;
        }
        Timer timer = Utils.timer();
        store.removePartition(partitions);
        if (log.isInfoEnabled()) {
            log.info("Removed {} partitions in {} ms",
                     partitions.size(), Utils.formatNum(timer.elapsedMillis()));
        }
    }

    private void partitionsAssigned(Collection<TopicPartition> partitions) {
        if (partitions.isEmpty()) {
            return;
        }
        Timer timer = Utils.timer();

        Map<TopicPartition, AssignmentConfig> configs =
                partitions.stream().collect(toMap(Function.identity(), ignored -> new AssignmentConfig(false)));
        store.addPartitions(configs);
        if (log.isInfoEnabled()) {
            log.info("Added {} partitions in {} ms",
                     partitions.size(), Utils.formatNum(timer.elapsedMillis()));
        }
    }
}
