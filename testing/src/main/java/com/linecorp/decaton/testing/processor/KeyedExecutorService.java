/*
 * Copyright 2024 LY Corporation
 *
 * LY Corporation licenses this file to you under the Apache License,
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

package com.linecorp.decaton.testing.processor;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class KeyedExecutorService implements AutoCloseable {
    private final ExecutorService[] executors;

    public KeyedExecutorService(int partitions) {
        executors = new ExecutorService[partitions];
        for (int i = 0; i < partitions; i++) {
            executors[i] = Executors.newSingleThreadExecutor();
        }
    }
    public void execute(int key, Runnable runnable) {
        int partition = Math.abs(key) % executors.length;
        executors[partition].execute(runnable);
    }

    @Override
    public void close() throws Exception {
        for (ExecutorService executor : executors) {
            executor.shutdown();
        }
    }
}
