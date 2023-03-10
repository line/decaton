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

package com.linecorp.decaton.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.junit.Rule;
import org.junit.Test;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import com.linecorp.decaton.protocol.Sample.HelloTask;

public class DecatonClientTest {
    @Rule
    public MockitoRule rule = MockitoJUnit.rule();

    @Spy
    private final DecatonClient<HelloTask> decaton = new DecatonClient<HelloTask>() {
        @Override
        public CompletableFuture<PutTaskResult> put(String key, HelloTask task) {
            return null;
        }

        @Override
        public CompletableFuture<PutTaskResult> put(String key, HelloTask task, long timestamp) {
            return null;
        }

        @Override
        public CompletableFuture<PutTaskResult> put(String key, HelloTask task,
                                                    TaskMetadata overrideTaskMetadata) {
            return null;
        }

        @Override
        public CompletableFuture<PutTaskResult> put(String key, HelloTask task,
                                                    TaskMetadata overrideTaskMetadata, Integer partition) {
            return null;
        }

        @Override
        public CompletableFuture<PutTaskResult> put(String key, HelloTask task,
                                                    Consumer<Throwable> errorCallback) {
            return null;
        }

        @Override
        public void close() throws Exception {
            // noop
        }
    };

    @Test
    public void testPutAsyncHelperOnSuccess() throws Exception {
        doReturn(CompletableFuture.completedFuture(null)).when(decaton)
                                                         .put(any(), any(HelloTask.class), anyLong());

        AtomicBoolean callbackCalled = new AtomicBoolean();
        CompletableFuture<PutTaskResult> result =
                decaton.put(null, HelloTask.getDefaultInstance(), 0L, e -> callbackCalled.set(true));
        assertFalse(callbackCalled.get());
        assertNull(result.get(1, TimeUnit.MILLISECONDS)); // should never throw
    }

    @Test
    public void testPutAsyncHelperOnException() throws Exception {
        CompletableFuture<PutTaskResult> future = new CompletableFuture<>();
        Throwable exception = new RuntimeException("runtime exception");
        future.completeExceptionally(exception);
        doReturn(future).when(decaton).put(any(), any(HelloTask.class), anyLong());

        AtomicReference<Throwable> callbackException = new AtomicReference<>();
        CompletableFuture<PutTaskResult> result =
                decaton.put(null, HelloTask.getDefaultInstance(), 0L, callbackException::set);

        assertEquals(exception, callbackException.get());
        Throwable innerException = null;
        try {
            result.get(1, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            innerException = e.getCause();
        }
        assertEquals(exception, innerException);
    }
}
