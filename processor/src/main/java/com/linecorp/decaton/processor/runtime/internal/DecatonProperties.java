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

import java.util.Optional;

import com.linecorp.decaton.processor.runtime.Property;
import com.linecorp.decaton.processor.runtime.PropertyDefinition;

/**
 * Represents a collection of {@link Property} that are subject as parameters for particular target.
 */
public interface DecatonProperties {
    /**
     * Returns associated {@link Property} for the {@link PropertyDefinition} given as an argument.
     * @param definition a {@link PropertyDefinition} to lookup associated {@link Property}.
     * @param <T> type of the property.
     * @return a {@link Property} associated to the given {@link PropertyDefinition}.
     *
     * @throws IllegalArgumentException when {@link PropertyDefinition} doesn't exists in properties.
     */
    default <T> Property<T> get(PropertyDefinition<T> definition) {
        return tryGet(definition).orElseThrow(() -> new IllegalArgumentException("no such property: " + definition));
    }

    /**
     * Returns associated {@link Property} for the {@link PropertyDefinition} given as an argument.
     * @param definition a {@link PropertyDefinition} to lookup associated {@link Property}.
     * @param <T> type of the property.
     * @return a {@link Property} associated to the given {@link PropertyDefinition} or {@link Optional#empty()} if not exist
     */
    <T> Optional<Property<T>> tryGet(PropertyDefinition<T> definition);
}
