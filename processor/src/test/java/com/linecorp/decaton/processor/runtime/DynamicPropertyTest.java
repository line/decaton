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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class DynamicPropertyTest {
    @Rule
    public MockitoRule rule = MockitoJUnit.rule();

    private static final PropertyDefinition<Long> LONG_PROPERTY =
            PropertyDefinition.define("num.property", Long.class, 0L,
                                      v -> v instanceof Long && (Long) v >= 0L);

    @Spy
    private final DynamicProperty<Long> prop = new DynamicProperty<>(LONG_PROPERTY);

    @Mock
    private BiConsumer<Long, Long> longPropertyListener;

    @Test
    public void testValue() {
        assertEquals(LONG_PROPERTY.defaultValue(), prop.value());

        prop.set(100L);
        assertEquals(100L, prop.value().longValue());
    }

    @Test
    public void testSet() {
        Long oldValue = prop.set(10L);
        assertEquals(0L, oldValue.longValue());
    }

    @Test
    public void testSetSameValue() {
        prop.listen(longPropertyListener);
        reset((Object) longPropertyListener);
        assertNull(prop.set(LONG_PROPERTY.defaultValue()));
        verify(longPropertyListener, never()).accept(any(), any());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetInvalidValue() {
        prop.set(-1L);
    }

    @Test
    public void testSetCallbacks() {
        prop.listen(longPropertyListener);
        prop.listen(longPropertyListener); // pretending different two callbacks

        AtomicLong setValueWhenCalled = new AtomicLong(-1L);
        doAnswer(invocation -> setValueWhenCalled.compareAndSet(-1L, prop.value()))
                .when(longPropertyListener).accept(any(), any());

        prop.set(1L);
        verify(longPropertyListener, times(2)).accept(LONG_PROPERTY.defaultValue(), Long.valueOf(1L));
        // property value should be up-to-date when callbacks are triggered.
        assertEquals(1L, setValueWhenCalled.get());
    }

    @Test
    public void testCheckingSet() {
        Long oldValue = prop.checkingSet(10L);
        assertEquals(0L, oldValue.longValue());
        assertEquals(10L, prop.value().longValue());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCheckingSetInvalidType() {
        prop.checkingSet("string");
    }
}
