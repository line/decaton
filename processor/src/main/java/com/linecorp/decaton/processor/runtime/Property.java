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

/**
 * Represents configurable parameters which can be supplied either statically or dynamically from external
 * sources.
 * Whenever arguments or fields of classes and methods are declared using this type, that indicates that
 * parameter can be supplied dynamically, and it might also indicates the parameter can be changed during
 * runtime to reflect parameter update without restarting the process.
 * Refer document upon each parameter to understand what exactly happens when the parameter got updated during
 * the runtime.
 *
 * To supply {@link Property} instance dynamically, you must configure your own {@link PropertySupplier} and
 * use {@link PropertySupplier#getProperty(PropertyDefinition)} to obtain an instance of bounded to the backend.
 *
 * To supply {@link Property} instance statically, accepting the value can't be changed unless you restart
 * the process, you can use {@link Property#ofStatic} method family to supply one from constant.
 *
 * @param <T> type of the property value.
 */
public interface Property<T> {
    /**
     * Returns {@link PropertyDefinition} which represents definition of this property instance.
     * @return a {@link PropertyDefinition}.
     */
    PropertyDefinition<T> definition();

    /**
     * Returns the current value associated to this property.
     * @return the current value.
     */
    T value();

    /**
     * Add a listener that listens to updates on this property value.
     *
     * A {@link BiConsumer} listener takes two arguments which are respectively, the old value and the new
     * value just had been set.
     *
     * Upon calling this method, the given {@code listener} invoked at least once to initialize the state with
     * the current value.
     *
     * If the underlying property is {@link StaticProperty}, as the value is static and will never be
     * changed, the given listener will never be called again.
     *
     * If the underlying property is {@link DynamicProperty} or anything other that supports updating value,
     * the given listener will gets called every time the value of this property gets updated.
     *
     * @param listener a {@link BiConsumer} which takes updates on this property value.
     */
    void listen(BiConsumer<T, T> listener);

    /**
     * Creates a {@link Property} which has static value.
     * @param definition a {@link PropertyDefinition} to associate with crated {@link Property}.
     * @param value value of the property.
     * @param <T> type of the property value.
     * @return a {@link Property} which has fixed value.
     */
    static <T> Property<T> ofStatic(PropertyDefinition<T> definition, T value) {
        return new StaticProperty<>(definition, value);
    }

    /**
     * Creates a {@link Property} which has static value initialized with
     * {@link PropertyDefinition#defaultValue()}.
     * @param definition a {@link PropertyDefinition} to associate with crated {@link Property}.
     * @param <T> type of the property value.
     * @return a {@link Property} which has fixed value.
     */
    static <T> Property<T> ofStatic(PropertyDefinition<T> definition) {
        return ofStatic(definition, definition.defaultValue());
    }
}
