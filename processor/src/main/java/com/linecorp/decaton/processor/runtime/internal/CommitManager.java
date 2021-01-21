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

import java.time.Clock;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import com.linecorp.decaton.processor.runtime.ProcessorProperties;
import com.linecorp.decaton.processor.runtime.Property;

import lombok.extern.slf4j.Slf4j;

/**
 * Coordinates consumer offset commits.
 */
@Slf4j
public class CommitManager {
    /**
     * Interface to offset store that provides offsets ready to be committed and receives offset that has been
     * committed for recording purpose.
     */
    interface OffsetsStore {
        /**
         * Return the map of {@link TopicPartition} to {@link OffsetAndMetadata} that are ready to be committed.
         * @return offset ready to be committed.
         */
        Map<TopicPartition, OffsetAndMetadata> commitReadyOffsets();

        /**
         * Report offsets that has been successfully committed.
         * @param offsets offsets that has been committed.
         */
        void storeCommittedOffsets(Map<TopicPartition, OffsetAndMetadata> offsets);
    }

    private final Consumer<?, ?> consumer;
    private final Property<Long> commitIntervalMillis;
    private final OffsetsStore store;
    private final Clock clock;

    private long lastCommittedMillis;
    private boolean asyncCommitInFlight;

    CommitManager(Consumer<?, ?> consumer,
                  Property<Long> commitIntervalMillis,
                  OffsetsStore store,
                  Clock clock) {
        this.consumer = consumer;
        this.commitIntervalMillis = commitIntervalMillis;
        this.store = store;
        this.clock = clock;
    }

    public CommitManager(Consumer<?, ?> consumer, Property<Long> commitIntervalMillis, OffsetsStore store) {
        this(consumer, commitIntervalMillis, store, Clock.systemDefaultZone());
    }

    /**
     * Commit ready offsets asynchronously if {@link ProcessorProperties#CONFIG_COMMIT_INTERVAL_MS} ms has been
     * elapsed since the last commit attempt.
     */
    public void maybeCommitAsync() {
        if (clock.millis() - lastCommittedMillis >= commitIntervalMillis.value()) {
            try {
                commitAsync();
            } finally {
                lastCommittedMillis = clock.millis();
            }
        }
    }

    /**
     * Commit ready offsets synchronously.
     * Errors are reported through exception and successful return of this method implies offsets are persisted
     * in Kafka.
     */
    public void commitSync() {
        Map<TopicPartition, OffsetAndMetadata> offsets = store.commitReadyOffsets();
        if (offsets.isEmpty()) {
            log.debug("No new offsets to commit, skipping commit");
            return;
        }

        log.debug("Committing offsets SYNC: {}", offsets);
        consumer.commitSync(offsets);
        store.storeCommittedOffsets(offsets);
    }

    /**
     * Commit ready offsets asynchronously.
     * Errors are not reported.
     * Subsequent calls of this method while there's still in-flight async commit are ignored.
     */
    public void commitAsync() {
        Map<TopicPartition, OffsetAndMetadata> offsets = store.commitReadyOffsets();
        if (offsets.isEmpty()) {
            log.debug("No new offsets to commit, skipping commit");
            return;
        }

        if (asyncCommitInFlight) {
            // We would end up making multiple OffsetCommit requests containing same set of offsets if we proceed
            // another async commit without waiting the first one to complete.
            // This would be harmless if `decaton.commit.interval.ms` is set to sufficiently large, but otherwise
            // we would make abusive offset commits despite there's no progress in processing.
            log.debug("Skipping commit due to another async commit is currently in-flight");
            return;
        }
        Thread callingThread = Thread.currentThread();
        consumer.commitAsync(offsets, (ignored, exception) -> {
            asyncCommitInFlight = false;
            if (exception != null) {
                log.warn("Offset commit failed asynchronously", exception);
                return;
            }
            if (Thread.currentThread() != callingThread) {
                // This isn't expected to happen (see below comment) but we check it with cheap cost
                // just in case to avoid silent corruption.
                log.error("BUG: commitAsync callback triggered by an unexpected thread: {}." +
                          " Please report this to Decaton developers", Thread.currentThread());
                return;
            }

            // Exception raised from the commitAsync callback bubbles up to Consumer.poll() so it kills the
            // subscription thread.
            // Basically the exception thrown from here is considered a bug, but failing to commit offset itself
            // isn't usually critical for consumer's continuation availability so we grab it here widely.
            try {
                // Some thread and timing safety consideration about below operation.
                // Thread safety:
                // Below operation touches some thread-unsafe resources such as ProcessingContexts map and
                // variable storing committed offset in ProcessingContext but we can assume this callback is
                // thread safe because in all cases a callback for commitAsync is called from inside of
                // Consumer.poll() which is called only by this (subscription) thread.
                // Timing safety:
                // At the time this callback is triggered there might be a change in ProcessingContexts map
                // underlying contexts. It would occur in two cases:
                // 1. When group rebalance is triggered and the context is dropped by revoke at partitionsRevoked().
                // 2. When dynamic processor reload is triggered and the context is renewed by
                // PartitionContexts.maybeHandlePropertyReload.
                // The case 2 is safe (safe to keep updating committed offset in renewed PartitionContext) because
                // it caries previously consuming offset without reset.
                // The case 1 is somewhat suspicious but should still be safe, because whenever partition revoke
                // happens it calls commitSync() through onPartitionsRevoked(). According to the document of
                // commitAsync(), it is guaranteed that its callback is called before subsequent commitSync()
                // returns. So when a context is dropped from PartitionContexts, there should be no in-flight
                // commitAsync().
                store.storeCommittedOffsets(offsets);
            } catch (RuntimeException e) {
                log.error("Unexpected exception caught while updating committed offset", e);
            }
        });
        asyncCommitInFlight = true;
    }
}
