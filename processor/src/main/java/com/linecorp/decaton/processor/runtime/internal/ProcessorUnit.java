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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import com.linecorp.decaton.processor.runtime.AsyncClosable;

import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Accessors(fluent = true)
public class ProcessorUnit implements AsyncClosable {
    @Getter
    private final int id;
    private final ProcessPipeline<?> pipeline;
    private final ExecutorService executor;
    private final AtomicInteger pendingTask;

    private volatile boolean terminated;

    public ProcessorUnit(ThreadScope scope, ProcessPipeline<?> pipeline, ExecutorService executor) {
        this.id = scope.threadId();
        this.pipeline = pipeline;
        this.executor = executor;

        pendingTask = new AtomicInteger();
    }

    public void putTask(TaskRequest request) {
        pendingTask.incrementAndGet();
        try {
            executor.execute(() -> processTask(request));
        } catch (RuntimeException e) {
            pendingTask.decrementAndGet();
            throw e;
        }
    }

    private void processTask(TaskRequest request) {
        if (terminated) {
            // There's a chance that some tasks leftover in executor's queue are still attempted to be processed
            // even after this unit enters shutdown sequences.
            // In such case we should ignore all following tasks to quickly complete shutdown sequence.
            return;
        }

        CompletionStage<Void> processCompletion;
        try {
            processCompletion = pipeline.scheduleThenProcess(request);
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            processCompletion = CompletableFuture.completedFuture(null);
            log.error("Error while processing request {}. Corresponding offset will be left uncommitted.",
                      request, e);
        }
        processCompletion.whenComplete((ignored, ignoredE) -> pendingTask.decrementAndGet());
    }

    public boolean hasPendingTasks() {
        return pendingTask.get() > 0;
    }

    @Override
    public CompletableFuture<Void> asyncClose() {
        terminated = true;
        pipeline.close();
        CompletableFuture<Void> shutdownComplete = new CompletableFuture<>();
        executor.submit(() -> {
            shutdownComplete.complete(null);
            log.debug("ProcessorUnit {} SHUTDOWN", id);
        });
        executor.shutdown();
        return shutdownComplete;
    }
}
