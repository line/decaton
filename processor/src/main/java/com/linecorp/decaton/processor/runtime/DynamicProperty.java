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

import java.util.function.BiConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linecorp.decaton.processor.runtime.internal.AbstractProperty;

/**
 * A {@link Property} implementation which holds dynamically changeable value.
 * @param <T> type of the property value.
 */
public class DynamicProperty<T> extends AbstractProperty<T> {
    private static final Logger logger = LoggerFactory.getLogger(DynamicProperty.class);
    private volatile T value;

    public DynamicProperty(PropertyDefinition<T> definition) {
        super(definition);
        set(definition.defaultValue());
    }

    @Override
    public T value() {
        return value;
    }

    @SuppressWarnings("unchecked")
    private static <T> T safeCast(Object value) {
        return (T) value;
    }

    /**
     * Update value of this property.
     *
     * This method first compares the given {@code value} with the current value using
     * {@link Object#equals(Object)}.
     * If two objects are different, new value's validity is checked by
     * {@link PropertyDefinition#isValid(Object)} and throw if the value considered invalid.
     * Otherwise, update the internal variable holding the value, and trigger listeners configured for this
     * property through {@link Property#listen(BiConsumer)}.
     *
     * Note that depending on each listener's processing time, because they are triggered one-by-one
     * synchronously this method may takes longer than we expect from it's name.
     *
     * @param value new value to set to this property.
     * @return old value which was set to this property. null if there was no update because the new value was
     * equivalent to the current value.
     *
     * @throws IllegalArgumentException when invalid value passed.
     */
    public synchronized T set(T value) {
        T currentValue = this.value;

        if (currentValue == null && value == null || currentValue != null && currentValue.equals(value)) {
            // No need to update.
            return null;
        }

        validate(value);

        this.value = value;
        logger.debug("Property {} has been updated ({} => {})", name(), currentValue, value);

        notifyListeners(currentValue, value);
        return currentValue;
    }

    /**
     * Update the value of this property, taking untyped object as an argument.
     *
     * This is a slightly different version of {@link #set}.
     * Before calling {@link #set}, this method check if the passed value's runtime class is matching to the
     * type configured as {@link PropertyDefinition#runtimeType()}.

     * @param value new value to set to this property.
     * @return old value which was set to this property. null if there was no update because the new value was
     * equivalent to the current value.
     *
     * @throws IllegalArgumentException when invalid value passed.
     */
    public T checkingSet(Object value) {
        Class<?> runtimeType = definition().runtimeType();

        if (value != null && runtimeType != null && !runtimeType.isInstance(value)) {
            throw new IllegalArgumentException(String.format(
                    "type %s is not applicable for property %s of type %s",
                    value.getClass().getCanonicalName(), name(), runtimeType.getCanonicalName()));
        }

        return set(safeCast(value));
    }
}
