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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import com.linecorp.decaton.processor.runtime.Property;
import com.linecorp.decaton.processor.runtime.PropertyDefinition;
import com.linecorp.decaton.processor.runtime.PropertySupplier;

public abstract class AbstractDecatonProperties implements DecatonProperties {
    private final Map<PropertyDefinition<?>, Property<?>> properties;

    public static class Builder<T extends AbstractDecatonProperties> {
        private final Function<Map<PropertyDefinition<?>, Property<?>>, T> constructor;
        private final Collection<PropertyDefinition<?>> definitions;
        private final Map<PropertyDefinition<?>, Property<?>> properties;

        public Builder(Function<Map<PropertyDefinition<?>, Property<?>>, T> constructor,
                       Collection<PropertyDefinition<?>> definitions) {
            this.constructor = constructor;
            this.definitions = definitions;
            properties = new HashMap<>();
        }

        void internalSet(Property<?> property) {
            properties.putIfAbsent(property.definition(), property);
        }

        public <P> Builder<T> set(Property<P> property) {
            internalSet(property);
            return this;
        }

        public Builder<T> setBySupplier(PropertySupplier supplier) {
            for (PropertyDefinition<?> definition : definitions) {
                supplier.getProperty(definition).ifPresent(this::internalSet);
            }
            return this;
        }

        public T build() {
            // Lastly fill up defaults for missing properties
            for (PropertyDefinition<?> definition : definitions) {
                internalSet(Property.ofStatic(definition));
            }
            return constructor.apply(properties);
        }
    }

    protected AbstractDecatonProperties(Map<PropertyDefinition<?>, Property<?>> properties) {
        this.properties = Collections.unmodifiableMap(properties);
    }

    @SuppressWarnings("unchecked")
    private static <T> T safeCast(Object value) {
        return (T) value;
    }

    @Override
    public <T> Optional<Property<T>> tryGet(PropertyDefinition<T> definition) {
        Property<?> prop = properties.get(definition);
        if (prop != null) {
            return Optional.of(safeCast(prop));
        }
        return Optional.empty();
    }
}
