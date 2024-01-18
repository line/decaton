/*
 * Copyright 2024 LY Corporation
 *
 * LY Corporation licenses this file to you under the Apache License,
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

package com.linecorp.decaton.testing;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ConcurrentHashSet<E> implements Set<E> {
    private static final Object PRESENT = new Object();
    private final ConcurrentMap<E, Object> inner = new ConcurrentHashMap<>();

    @Override
    public int size() {
        return inner.size();
    }

    @Override
    public boolean isEmpty() {
        return inner.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return inner.containsKey(o);
    }

    @Override
    public Iterator<E> iterator() {
        return inner.keySet().iterator();
    }

    @Override
    public Object[] toArray() {
        return inner.keySet().toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return inner.keySet().toArray(a);
    }

    @Override
    public boolean add(E e) {
        return inner.put(e, PRESENT) != PRESENT;
    }

    @Override
    public boolean remove(Object o) {
        return inner.remove(o) == PRESENT;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return inner.keySet().containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        boolean added = false;
        for (E e : c) {
            added |= add(e);
        }
        return added;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return inner.keySet().retainAll(c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return inner.keySet().removeAll(c);
    }

    @Override
    public void clear() {
        inner.clear();
    }

    @Override
    public int hashCode() {
        return inner.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return inner.keySet().equals(obj);
    }
}
