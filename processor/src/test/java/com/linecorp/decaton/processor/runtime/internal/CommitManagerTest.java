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

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.time.Clock;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import com.linecorp.decaton.processor.runtime.DynamicProperty;
import com.linecorp.decaton.processor.runtime.ProcessorProperties;
import com.linecorp.decaton.processor.runtime.internal.CommitManager.OffsetsStore;

public class CommitManagerTest {
    @Rule
    public MockitoRule rule = MockitoJUnit.rule();

    @Mock
    private Consumer<byte[], byte[]> consumer;

    private final DynamicProperty<Long> commitIntervalMillis =
            new DynamicProperty<>(ProcessorProperties.CONFIG_COMMIT_INTERVAL_MS);

    @Mock
    private OffsetsStore store;

    @Mock
    private Clock clock;

    private CommitManager commitManager;

    @Before
    public void setUp() {
        commitManager = spy(new CommitManager(consumer, commitIntervalMillis, store, clock));
    }
    @SuppressWarnings("unchecked")
    @Test(timeout = 5000)
    public void testCommitCompletedOffsetsSync() {
        // When committed ended up successfully update committed offsets
        Map<TopicPartition, OffsetAndMetadata> offsets = singletonMap(
                new TopicPartition("topic", 0), new OffsetAndMetadata(1234, null));
        doReturn(offsets).when(store).commitReadyOffsets();

        commitManager.commitSync();
        verify(consumer, times(1)).commitSync(any(Map.class));
        verify(store, times(1)).storeCommittedOffsets(offsets);
    }

    @SuppressWarnings("unchecked")
    @Test(timeout = 5000)
    public void testCommitCompletedOffsetsSync_NO_COMMIT() {
        // When target offsets is empty do not attempt any commit
        doReturn(emptyMap()).when(store).commitReadyOffsets();

        commitManager.commitSync();
        verify(consumer, never()).commitSync(any(Map.class));
        verify(consumer, never()).commitAsync(any(Map.class), any());
    }

    @SuppressWarnings("unchecked")
    @Test(timeout = 5000)
    public void testCommitCompletedOffsetsSync_FAIL() {
        // When commit raised an exception do not update committed offsets
        Map<TopicPartition, OffsetAndMetadata> offsets = singletonMap(
                new TopicPartition("topic", 0), new OffsetAndMetadata(1234, null));
        doReturn(offsets).when(store).commitReadyOffsets();
        doThrow(new RuntimeException("error")).when(consumer).commitSync(any(Map.class));
        try {
            commitManager.commitSync();
        } catch (RuntimeException ignored) {
            // ignore
        }
        verify(store, never()).storeCommittedOffsets(any());
    }

    @SuppressWarnings("unchecked")
    @Test(timeout = 5000)
    public void testCommitCompletedOffsetsAsync() throws InterruptedException {
        Map<TopicPartition, OffsetAndMetadata> offsets = singletonMap(
                new TopicPartition("topic", 0), new OffsetAndMetadata(1234, null));
        doReturn(offsets).when(store).commitReadyOffsets();
        AtomicReference<OffsetCommitCallback> cbRef = new AtomicReference<>();
        doAnswer(invocation -> {
            OffsetCommitCallback cb = invocation.getArgument(1);
            cbRef.set(cb);
            return null;
        }).when(consumer).commitAsync(any(Map.class), any());

        commitManager.commitAsync();

        // Committed offsets should not be updated yet here
        verify(consumer, times(1)).commitAsync(any(Map.class), any());
        verify(store, never()).storeCommittedOffsets(any());

        // Subsequent async commit attempt should be ignored until the in-flight one completes
        commitManager.commitAsync();
        verify(consumer, times(1)).commitAsync(any(Map.class), any());
        verify(store, never()).storeCommittedOffsets(any());

        // Committed offset should be updated once the in-flight request completes
        cbRef.get().onComplete(offsets, null);
        verify(store, times(1)).storeCommittedOffsets(offsets);
    }

    @SuppressWarnings("unchecked")
    @Test(timeout = 5000)
    public void testCommitCompletedOffsetsAsync_FAIL() throws InterruptedException {
        Map<TopicPartition, OffsetAndMetadata> offsets = singletonMap(
                new TopicPartition("topic", 0), new OffsetAndMetadata(1234, null));
        doReturn(offsets).when(store).commitReadyOffsets();
        AtomicReference<OffsetCommitCallback> cbRef = new AtomicReference<>();
        doAnswer(invocation -> {
            OffsetCommitCallback cb = invocation.getArgument(1);
            cbRef.set(cb);
            return null;
        }).when(consumer).commitAsync(any(Map.class), any());

        commitManager.commitAsync();
        // If async commit fails it should never update committed offset
        cbRef.get().onComplete(offsets, new RuntimeException("failure"));
        verify(store, never()).storeCommittedOffsets(any());
    }

    @SuppressWarnings("unchecked")
    @Test(timeout = 5000)
    public void testCommitCompletedOffsetsAsync_SUBSEQUENT_SYNC() {
        Map<TopicPartition, OffsetAndMetadata> offsets = singletonMap(
                new TopicPartition("topic", 0), new OffsetAndMetadata(1234, null));
        doReturn(offsets).when(store).commitReadyOffsets();

        // No one completes async commit underlying this.
        commitManager.commitAsync();

        // Subsequent sync commit can proceed regardless of in-flight async commit
        commitManager.commitSync();
        verify(consumer, times(1)).commitSync(any(Map.class));
    }

    @Test
    public void testMaybeCommitAsync() {
        commitIntervalMillis.set(1000L);
        Map<TopicPartition, OffsetAndMetadata> offsets = singletonMap(
                new TopicPartition("topic", 0), new OffsetAndMetadata(1234, null));
        doReturn(offsets).when(store).commitReadyOffsets();

        doReturn(1000L).when(clock).millis();
        commitManager.maybeCommitAsync();
        verify(commitManager, times(1)).commitAsync();

        doReturn(1001L).when(clock).millis();
        commitManager.maybeCommitAsync();
        // No new commits.
        verify(commitManager, times(1)).commitAsync();

        doReturn(1000L + commitIntervalMillis.value()).when(clock).millis();
        commitManager.maybeCommitAsync();
        verify(commitManager, times(2)).commitAsync();
    }
}
