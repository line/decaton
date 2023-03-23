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
import static org.junit.Assert.assertSame;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import org.junit.Test;

import com.linecorp.decaton.processor.runtime.Property;
import com.linecorp.decaton.processor.runtime.PropertyDefinition;
import com.linecorp.decaton.processor.runtime.StaticPropertySupplier;

public class AbstractDecatonPropertiesTest {
    private static final PropertyDefinition<Long> LONG_PROPERTY =
            PropertyDefinition.define("num.property", Long.class, 0L,
                                      v -> v instanceof Long && (Long) v >= 0L);
    private static final PropertyDefinition<Integer> INT_PROPERTY =
            PropertyDefinition.define("int.property", Integer.class, 1,
                                      v -> v instanceof Integer);
    private static final PropertyDefinition<String> STRING_PROPERTY =
            PropertyDefinition.define("str.property", String.class, "abc",
                                      v -> v instanceof String);

    private static final Property<Long> longProperty = Property.ofStatic(LONG_PROPERTY, 1234L);

    private static class TestProperties extends AbstractDecatonProperties {
        protected TestProperties(Map<PropertyDefinition<?>, Property<?>> properties) {
            super(properties);
        }

        static TestProperties.Builder<TestProperties> builder() {
            return new TestProperties.Builder<>(TestProperties::new,
                                                Arrays.asList(LONG_PROPERTY, INT_PROPERTY, STRING_PROPERTY));
        }
    }

    private final TestProperties props = new TestProperties(
            Collections.singletonMap(LONG_PROPERTY, longProperty));

    @Test
    public void testGet() {
        Property<Long> prop = props.get(LONG_PROPERTY);
        assertSame(longProperty, prop);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetAbsentProperty() {
        props.get(PropertyDefinition.define("absent.key", Long.class, 0L));
    }

    @Test
    public void testTryGetAbsentProperty() {
        assertFalse(props.tryGet(PropertyDefinition.define("absent.key", Long.class, 0L)).isPresent());
    }

    @Test
    public void testBuilder() {
        TestProperties props = TestProperties.builder()
                                             .setBySupplier(StaticPropertySupplier.of(longProperty))
                                             .setBySupplier(
                                                     StaticPropertySupplier.of(
                                                             Property.ofStatic(LONG_PROPERTY, 5678L),
                                                             Property.ofStatic(
                                                                     INT_PROPERTY, 4649)))
                                             .build();

        assertEquals(1234L, props.get(LONG_PROPERTY).value().longValue());
        assertEquals(4649, props.get(INT_PROPERTY).value().intValue());
        assertEquals("abc", props.get(STRING_PROPERTY).value());
    }
}
