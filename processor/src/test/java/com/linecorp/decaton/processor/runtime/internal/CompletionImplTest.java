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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.linecorp.decaton.processor.Completion.TimeoutChoice;

public class CompletionImplTest {

    @Test
    public void completeWithComplete() {
        CompletionImpl parent = new CompletionImpl();
        CompletionImpl child = new CompletionImpl();
        parent.completeWith(child);

        assertFalse(parent.isComplete());
        child.complete();
        assertTrue(parent.isComplete());
    }

    @Test
    public void completeWithTimeout() {
        CompletionImpl parent = new CompletionImpl();
        CompletionImpl child = new CompletionImpl();
        parent.completeWith(child);

        AtomicInteger parentCbCount = new AtomicInteger();
        parent.expireCallback(comp -> {
            parentCbCount.getAndIncrement();
            return TimeoutChoice.GIVE_UP;
        });
        AtomicInteger childCbCount = new AtomicInteger();
        child.expireCallback(comp -> {
            if (childCbCount.getAndIncrement() == 0) {
                return TimeoutChoice.EXTEND;
            } else {
                return TimeoutChoice.GIVE_UP;
            }
        });

        assertFalse(parent.tryExpire());
        assertEquals(0, parentCbCount.get());
        assertEquals(1, childCbCount.get());

        assertTrue(parent.tryExpire());
        assertEquals(1, parentCbCount.get());
        assertEquals(2, childCbCount.get());

        // Make sure over-calling tryExpire won't cause extra callback invocations.
        parent.complete();
        assertTrue(parent.tryExpire());
        assertEquals(1, parentCbCount.get());
        assertEquals(2, childCbCount.get());
    }

    @Test
    public void tryExpireWithoutCallback() {
        CompletionImpl comp = new CompletionImpl();
        assertTrue(comp.tryExpire());
    }

    @Test
    public void complete() {
        CompletionImpl comp = new CompletionImpl();
        assertFalse(comp.isComplete());
        comp.complete();
        assertTrue(comp.isComplete());
    }
}
