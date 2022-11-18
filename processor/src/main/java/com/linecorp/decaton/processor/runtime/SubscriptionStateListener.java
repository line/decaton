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

import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.RebalanceProtocol;

/**
 * An interface to listen state changes of {@link ProcessorSubscription}.
 * This interface is intended to be used for health check or integration testing purpose.
 *
 * For example, you can make sure that Decaton has started to process as follows:
 * <pre>
 * {@code
 * CountDownLatch latch = new CountDownLatch(1);
 * SubscriptionBuilder.newBuilder("my-subscription")
 *                    // ...
 *                    .stateListener(state -> {
 *                        if (state == State.RUNNING) {
 *                            latch.countDown();
 *                        }
 *                    }).buildAndStart();
 * latch.await();
 * // actual integration testing continues...
 * }
 * </pre>
 */
@FunctionalInterface
public interface SubscriptionStateListener {
    /**
     * Represents possible states that a {@link ProcessorSubscription} can be in.
     * The expected state transition is:
     * <pre>
     * {@code
     *                  ┌───┐
     *                  │   v
     * INITIALIZING -> RUNNING <-> REBALANCING ┐
     *                    │                    v
     *                    └──────> SHUTTING_DOWN -> TERMINATED
     * }
     * </pre>
     *
     * Listener will be called at each transition in the flow.
     * In addition, listener will be also called with {@link #INITIALIZING} state when Decaton starts initialization sequence.
     */
    enum State {
        /**
         * Initializing subscription internals.
         */
        INITIALIZING,
        /**
         * Consumer group rebalance is running.
         * Meanwhile no extra records are fetched until it completes in {@link RebalanceProtocol#EAGER} rebalancing.
         * In {@link RebalanceProtocol#COOPERATIVE} rebalancing, records for non-revoked partitions may be fetched.
         */
        REBALANCING,
        /**
         * Started to process tasks.
         * Fetching records from brokers and feeding them into partition processors.
         * Note that in {@link RebalanceProtocol#COOPERATIVE} rebalancing, {@link #RUNNING} may transition to
         * {@link #RUNNING} when there's no partition revocation.
         */
        RUNNING,
        /**
         * Entered shutdown sequence.
         * No extra tasks will be queued, but the tasks that are already in process continues till it completes.
         */
        SHUTTING_DOWN,
        /**
         * All shutdown sequence has done. The subscription is terminated completely.
         */
        TERMINATED,
    }

    /**
     * Called at state transitioned to new state.
     * This method is called only from subscription thread (i.e. {@link ProcessorSubscription} itself)
     * @param newState new state of the subscription
     */
    void onChange(State newState);
}
