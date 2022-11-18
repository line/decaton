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

package com.linecorp.decaton.processor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.ClassRule;
import org.junit.Test;

import com.linecorp.decaton.processor.runtime.SubscriptionStateListener.State;
import com.linecorp.decaton.testing.KafkaClusterRule;
import com.linecorp.decaton.testing.processor.ProcessorTestSuite;

public class SubscriptionStateTest {
    @ClassRule
    public static KafkaClusterRule rule = new KafkaClusterRule();

    @Test(timeout = 30000)
    public void testStateTransition() throws Exception {
        Map<Integer, List<State>> subscriptionStates = new HashMap<>();
        ProcessorTestSuite
                .builder(rule)
                .configureProcessorsBuilder(builder -> builder.thenProcess((ctx, task) -> {}))
                .statesListener((instanceId, newState) -> {
                    synchronized (subscriptionStates) {
                        subscriptionStates.computeIfAbsent(instanceId, key -> new ArrayList<>()).add(newState);
                    }
                })
                .build()
                .run();

        subscriptionStates.forEach((instanceId, stateHistory) -> {
            assertEquals(State.INITIALIZING, stateHistory.get(0));
            assertEquals(State.TERMINATED, stateHistory.get(stateHistory.size() - 1));

            State state;
            Deque<State> states = new ArrayDeque<>(stateHistory);
            List<State> validTransition = Arrays.asList(State.INITIALIZING);
            while ((state = states.pollFirst()) != null) {
                if (!validTransition.contains(state)) {
                    fail(String.format("Invalid state transition %s on subscription-%d", stateHistory, instanceId));
                }
                switch (state) {
                    case INITIALIZING:
                        validTransition = Arrays.asList(State.RUNNING);
                        break;
                    case REBALANCING:
                        // It's possible to transition to SHUTTING_DOWN from REBALANCING
                        // when onPartitionRevoked and onPartitionAssigned are not done in same poll() and
                        // shutdown is initiated between them
                        validTransition = Arrays.asList(State.RUNNING, State.SHUTTING_DOWN);
                        break;
                    case RUNNING:
                        validTransition = Arrays.asList(State.REBALANCING, State.SHUTTING_DOWN);
                        break;
                    case SHUTTING_DOWN:
                        validTransition = Arrays.asList(State.TERMINATED);
                        break;
                    case TERMINATED:
                        // happen when subscription is restarted
                        validTransition = Arrays.asList(State.INITIALIZING);
                        break;
                }
            }
        });
    }
}
