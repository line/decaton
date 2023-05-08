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

package com.linecorp.decaton.processor.runtime;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.TopicPartition;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import com.linecorp.decaton.processor.DeferredCompletion;
import com.linecorp.decaton.processor.metrics.Metrics;
import com.linecorp.decaton.processor.runtime.internal.OffsetState;
import com.linecorp.decaton.processor.runtime.internal.OffsetStateReaper;
import com.linecorp.decaton.processor.runtime.internal.OutOfOrderCommitControl;

/**
 * Benchmark performance of {@link OutOfOrderCommitControl}.
 *
 * In this benchmark we try to simulate a realistic use of it as possible as we can.
 *
 * There's always just 1 thread (handling consumer poll) that calls
 * {@link OutOfOrderCommitControl#reportFetchedOffset(long)} and
 * {@link OutOfOrderCommitControl#updateHighWatermark()}.
 *
 * On the other hand there are multiple threads calling {@link DeferredCompletion#complete()} as many
 * as the {@link ProcessorProperties#CONFIG_PARTITION_CONCURRENCY} is configured to.
 *
 * In this benchmark we employ the scenario as below.
 * - Consumer loop thread polls {@link #BATCH_SIZE} records until it becomes full and feed their offsets into
 *   it.
 * - They are passed into threads pool consists of {@link #NUM_WORKER_THREADS}, and then completed immediately.
 * - Consumer loop thread calls {@link OutOfOrderCommitControl#updateHighWatermark()} after feeding
 *   {@link #BATCH_SIZE} and calls {@link OutOfOrderCommitControl#commitReadyOffset()}.
 * - Loop the above steps until {@link OutOfOrderCommitControl#commitReadyOffset()} returns the value equals to
 *   {@link #NUM_OFFSETS}.
 * - Measure entire execution duration as a performance indicator.
 */
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 1)
@Measurement(iterations = 2)
public class OutOfOrderCommitControlBenchmark {
    private static final TopicPartition topicPartition = new TopicPartition("topic", 0);
    private static final int CAPACITY = 10_000;

    public static final long NUM_OFFSETS = 1_000_000;
    public static final long BATCH_SIZE = 1_000;
    public static final int NUM_WORKER_THREADS = 20;

    abstract static class BmState<T> {
        T control;
        ExecutorService workers;

        abstract T createCommitControl();

        @Setup(Level.Invocation)
        public void setUp() {
            control = createCommitControl();
            workers = Executors.newFixedThreadPool(NUM_WORKER_THREADS);
        }

        @TearDown(Level.Invocation)
        public void tearDown() throws InterruptedException {
            workers.shutdown();
            workers.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
            control = null;
        }
    }

    @State(Scope.Thread)
    public static class BmStateV1 extends BmState<OutOfOrderCommitControlV1> {
        @Override
        OutOfOrderCommitControlV1 createCommitControl() {
            return new OutOfOrderCommitControlV1(topicPartition);
        }
    }

    @Benchmark
    public void outOfOrderCommitControlV1(BmStateV1 state) throws InterruptedException {
        OutOfOrderCommitControlV1 control = state.control;

        for (long offset = 1; offset <= NUM_OFFSETS; ) {
            boolean noProgress = true;

            for (int i = 0; i < BATCH_SIZE; i++, offset++) {
                if (control.pendingRecordsCount() >= CAPACITY) {
                    break;
                }
                noProgress = false;
                control.reportFetchedOffset(offset);

                long offsetSnapshot = offset;
                state.workers.execute(() -> control.complete(offsetSnapshot));
            }
            if (noProgress) {
                Thread.yield();
            }
        }

        while (control.commitReadyOffset() < NUM_OFFSETS) {
            Thread.yield();
        }
    }

    @State(Scope.Thread)
    public static class BmStateV2 extends BmState<OutOfOrderCommitControlV2> {
        @Override
        OutOfOrderCommitControlV2 createCommitControl() {
            return new OutOfOrderCommitControlV2(topicPartition, CAPACITY);
        }
    }

    @Benchmark
    public void outOfOrderCommitControlV2(BmStateV2 state) throws InterruptedException {
        OutOfOrderCommitControlV2 control = state.control;

        for (long offset = 1; offset <= NUM_OFFSETS; ) {
            boolean noProgress = true;

            for (int i = 0; i < BATCH_SIZE; i++, offset++) {
                if (control.pendingOffsetsCount() >= CAPACITY) {
                    break;
                }
                noProgress = false;
                control.reportFetchedOffset(offset);

                long offsetSnapshot = offset;
                state.workers.execute(() -> control.complete(offsetSnapshot));
            }
            if (noProgress) {
                Thread.yield();
            }
            control.updateHighWatermark();
        }

        control.updateHighWatermark();
        while (control.commitReadyOffset() < NUM_OFFSETS) {
            Thread.yield();
            control.updateHighWatermark();
        }
    }

    @State(Scope.Thread)
    public static class BmStateV3 extends BmState<OutOfOrderCommitControl> {
        @Override
        OutOfOrderCommitControl createCommitControl() {
            OffsetStateReaper reaper = new OffsetStateReaper(
                    Property.ofStatic(ProcessorProperties.CONFIG_DEFERRED_COMPLETE_TIMEOUT_MS),
                    Metrics.withTags("subscription", "subsc", "topic", "topic", "partition", "1")
                            .new CommitControlMetrics());
            return new OutOfOrderCommitControl(topicPartition, CAPACITY, reaper);
        }
    }

    @Benchmark
    public void outOfOrderCommitControlV3(BmStateV3 state) throws InterruptedException {
        OutOfOrderCommitControl control = state.control;

        for (long offset = 1; offset <= NUM_OFFSETS; ) {
            boolean noProgress = true;

            for (int i = 0; i < BATCH_SIZE; i++, offset++) {
                if (control.pendingOffsetsCount() >= CAPACITY) {
                    break;
                }
                noProgress = false;
                OffsetState offsetState = control.reportFetchedOffset(offset);

                state.workers.execute(offsetState.completion()::complete);
            }
            if (noProgress) {
                Thread.yield();
            }
            control.updateHighWatermark();
        }

        control.updateHighWatermark();
        while (control.commitReadyOffset() < NUM_OFFSETS) {
            Thread.yield();
            control.updateHighWatermark();
        }
    }
}
