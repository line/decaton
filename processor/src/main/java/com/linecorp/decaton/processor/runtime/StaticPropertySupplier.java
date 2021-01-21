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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * A {@link PropertySupplier} implementation that simply holds a fixed list of properties.
 */
public class StaticPropertySupplier implements PropertySupplier {
    private final Map<PropertyDefinition<?>, Property<?>> properties;

    /**
     * Create a new {@link StaticPropertySupplier} with given properties.
     * The argument property list must not contain duplicate entry for the same property definition.
     *
     * @param properties list of {@link Property} instances to hold
     * @return a {@link StaticPropertySupplier}
     * @throws IllegalArgumentException when the list contains duplicate entries for the same property key
     */
    public static StaticPropertySupplier of(Property<?>... properties) {
        return of(Arrays.asList(properties));
    }

    /**
     * Create a new {@link StaticPropertySupplier} with given properties.
     * The argument property list must not contain duplicate entry for the same property definition.
     *
     * @param properties list of {@link Property} instances to hold
     * @return a {@link StaticPropertySupplier}
     * @throws IllegalArgumentException when the list contains duplicate entries for the same property key
     */
    public static StaticPropertySupplier of(Collection<Property<?>> properties) {
        Map<PropertyDefinition<?>, Property<?>> props = new HashMap<>();

        for (Property<?> prop : properties) {
            Property<?> oldEntry = props.put(prop.definition(), prop);
            if (oldEntry != null) {
                throw new IllegalArgumentException("properties list contains duplicate definition");
            }
        }

        return new StaticPropertySupplier(props);
    }

    StaticPropertySupplier(Map<PropertyDefinition<?>, Property<?>> properties) {
        this.properties = properties;
    }

    @SuppressWarnings("unchecked")
    private static <T> T forceCast(Object o) {
        return (T) o;
    }

    @Override
    public <T> Optional<Property<T>> getProperty(PropertyDefinition<T> definition) {
        return Optional.ofNullable(forceCast(properties.get(definition)));
    }
}
