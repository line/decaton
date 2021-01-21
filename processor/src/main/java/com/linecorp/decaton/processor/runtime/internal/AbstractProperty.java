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

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linecorp.decaton.processor.runtime.Property;
import com.linecorp.decaton.processor.runtime.PropertyDefinition;

public abstract class AbstractProperty<T> implements Property<T> {
    private static final Logger logger = LoggerFactory.getLogger(AbstractProperty.class);

    private final PropertyDefinition<T> definition;
    private final List<BiConsumer<T, T>> listeners;

    protected AbstractProperty(PropertyDefinition<T> definition) {
        this.definition = definition;
        listeners = new ArrayList<>();
    }

    @Override
    public PropertyDefinition<T> definition() {
        return definition;
    }

    @Override
    public void listen(BiConsumer<T, T> listener) {
        synchronized (listeners) {
            listeners.add(listener);
        }
        // On registration, listener receives notification for initializing value reference.
        notifyListener(listener, null, value());
    }

    protected String name() {
        return definition.name();
    }

    protected void validate(Object value) {
        if (!definition.isValid(value)) {
            throw new IllegalArgumentException(String.format(
                    "invalid value %s attempted to set for property %s", value, name()));
        }
    }

    private void notifyListener(BiConsumer<T, T> listener, T oldValue, T newValue) {
        try {
            listener.accept(oldValue, newValue);
        } catch (RuntimeException e) {
            logger.warn("Listener threw uncaught exception for value update of {}", name(), e);
        }
    }

    protected void notifyListeners(T oldValue, T newValue) {
        synchronized (listeners) {
            listeners.forEach(l -> notifyListener(l, oldValue, newValue));
        }
    }
}
