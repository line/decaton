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

package com.linecorp.decaton.processor.runtime.internal;

import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class UtilsTest {

    @Test
    @Timeout(5)
    void completeOnTimeoutOK() {
        CompletableFuture<Object> fut = new CompletableFuture<>();
        Object success = new Object();
        Object timeout = new Object();
        Utils.completeOnTimeout(fut, timeout, 1000);
        fut.complete(success);
        assertSame(success, fut.join());
    }

    @Test
    @Timeout(5)
    void completeOnTimeoutTO() {
        CompletableFuture<Object> fut = new CompletableFuture<>();
        Object timeout = new Object();
        Utils.completeOnTimeout(fut, timeout, 1000);
        assertSame(timeout, fut.join());
    }
}
