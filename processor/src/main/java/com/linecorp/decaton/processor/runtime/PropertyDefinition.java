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

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

import lombok.Getter;
import lombok.experimental.Accessors;

/**
 * Represents definition of a {@link Property}. This instance annotates {@link Property}, naming it,
 * declaring it's runtime type and the default value, and providing validation on the value if necessary.
 *
 * @param <T> the type of associated {@link Property}.
 */
@Accessors(fluent = true)
public class PropertyDefinition<T> {
    /**
     * Name of the property.
     */
    @Getter
    private final String name;
    /**
     * Runtime type of the property.
     */
    @Getter
    private final Class<?> runtimeType;
    /**
     * The default value of the property. May be null if it's acceptable.
     */
    @Getter
    private final T defaultValue;
    private final Predicate<Object> validator;

    /**
     * Creates a {@link PropertyDefinition}.
     *
     * @param name name of the property.
     * @param runtimeType runtime type of the property.
     * @param <T> type of the property.
     * @return a {@link PropertyDefinition}.
     */
    public static <T> PropertyDefinition<T> define(String name, Class<?> runtimeType) {
        return define(name, runtimeType, null);
    }

    /**
     * Creates a {@link PropertyDefinition}.
     *
     * @param name name of the property.
     * @param runtimeType runtime type of the property.
     * @param defaultValue the default value of the property.
     * @param <T> type of the property.
     * @return a {@link PropertyDefinition}.
     */
    public static <T> PropertyDefinition<T> define(String name, Class<?> runtimeType, T defaultValue) {
        return define(name, runtimeType, defaultValue, v -> true);
    }

    /**
     * Creates a {@link PropertyDefinition}.
     *
     * @param name name of the property.
     * @param runtimeType runtime type of the property.
     * @param defaultValue the default value of the property.
     * @param validator a {@link Function} which returns boolean wheres true indicates valid and false
     * indicates invalid taking an {@link Object} the value of property as an argument.
     * @param <T> type of the property.
     * @return a {@link PropertyDefinition}.
     */
    public static <T> PropertyDefinition<T> define(
            String name,
            Class<?> runtimeType,
            T defaultValue,
            Predicate<Object> validator) {
        return new PropertyDefinition<>(name, runtimeType, defaultValue, validator);
    }

    PropertyDefinition(String name, Class<?> runtimeType, T defaultValue,
                       Predicate<Object> validator) {
        this.name = name;
        this.runtimeType = runtimeType;
        this.defaultValue = defaultValue;
        this.validator = validator;
    }

    /**
     * Returns an optional which might contains validator configured for this definition.
     * @return a {@link Function} validates property value's validity.
     */
    public Optional<Predicate<Object>> validator() {
        return Optional.ofNullable(validator);
    }

    /**
     * Validates if the given value is valid to be set for the property associated to this definition.
     * @param value value to validate.
     * @return true if the value is valid. false otherwise.
     */
    public boolean isValid(Object value) {
        return validator().map(f -> f.test(value)).orElse(true);
    }

    /**
     * Returns validator function which validates if the value is an instance of {@link List} and its each
     * element satisfies type boundary of given argument.
     * @param clazz expected type of each list element.
     * @return validator which checks object's validity.
     */
    @SuppressWarnings("unchecked")
    public static Predicate<Object> checkListElement(Class<?> clazz) {
        return v -> v instanceof List && ((List<Object>) v).stream().allMatch(clazz::isInstance);
    }
}
