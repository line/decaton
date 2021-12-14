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

package com.linecorp.decaton.centraldogma;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

import com.linecorp.centraldogma.client.CentralDogma;
import com.linecorp.centraldogma.client.Watcher;
import com.linecorp.centraldogma.common.Change;
import com.linecorp.centraldogma.common.PushResult;
import com.linecorp.centraldogma.common.Query;
import com.linecorp.centraldogma.common.Revision;
import com.linecorp.decaton.processor.runtime.DynamicProperty;
import com.linecorp.decaton.processor.runtime.ProcessorProperties;
import com.linecorp.decaton.processor.runtime.Property;
import com.linecorp.decaton.processor.runtime.PropertyDefinition;
import com.linecorp.decaton.processor.runtime.PropertySupplier;
import com.linecorp.decaton.processor.runtime.StaticPropertySupplier;

public class CentralDogmaPropertySupplierTest {
    @Rule
    public MockitoRule rule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

    private final ObjectMapper objectMapper = new ObjectMapper();

    private static final String PROJECT_NAME = "unit-test";
    private static final String REPOSITORY_NAME = "repo";
    private static final String FILENAME = "/subscription.json";

    private static final PropertyDefinition<Long> LONG_PROPERTY =
            PropertyDefinition.define("num.property", Long.class, 0L,
                                      v -> v instanceof Long && (Long) v >= 0L);

    private static final PropertyDefinition<List<String>> LIST_PROPERTY =
            PropertyDefinition.define("list.property", List.class, Collections.emptyList(),
                                      PropertyDefinition.checkListElement(String.class));

    @Mock
    private CentralDogma centralDogma;

    @Mock
    Watcher<JsonNode> rootWatcher;

    private CentralDogmaPropertySupplier supplier;

    @Before
    public void setUp() {
        when(centralDogma.fileWatcher(PROJECT_NAME, REPOSITORY_NAME, Query.ofJsonPath(FILENAME)))
                .thenReturn(rootWatcher);

        supplier = new CentralDogmaPropertySupplier(centralDogma, PROJECT_NAME, REPOSITORY_NAME, FILENAME);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testWatcherSetup() {
        when(rootWatcher.latestValue()).thenReturn(
                objectMapper.createObjectNode().put(LONG_PROPERTY.name(), 123L));

        Watcher<JsonNode> longPropertyWatcher = mock(Watcher.class);
        Watcher<JsonNode> listPropertyWatcher = mock(Watcher.class);

        when(rootWatcher.newChild((Query<JsonNode>) any()))
                .thenReturn(longPropertyWatcher)
                .thenReturn(listPropertyWatcher)
                .thenReturn(null);

        assertTrue(supplier.getProperty(LONG_PROPERTY).isPresent());

        verify(rootWatcher).newChild(any());
        verify(longPropertyWatcher).watch(any(Consumer.class));
    }

    @Test
    public void testConvertValue() {
        JsonNodeFactory factory = objectMapper.getNodeFactory();

        Object convertedLong = supplier.convertNodeToValue(
                new DynamicProperty<>(LONG_PROPERTY), factory.numberNode(10L));

        assertSame(Long.class, convertedLong.getClass());
        assertEquals(10L, convertedLong);

        Object convertedList = supplier.convertNodeToValue(new DynamicProperty<>(LIST_PROPERTY),
                                                           factory.arrayNode().add("foo").add("bar"));
        assertEquals(Arrays.asList("foo", "bar"), convertedList);
    }

    @Test
    public void testSetValue() {
        JsonNodeFactory factory = objectMapper.getNodeFactory();

        DynamicProperty<Long> prop = spy(new DynamicProperty<>(LONG_PROPERTY));
        supplier.setValue(prop, factory.numberNode(10L));
        verify(prop).checkingSet(10L);
    }

    @Test
    public void testGetPropertyAbsentName() {
        when(rootWatcher.latestValue()).thenReturn(objectMapper.createObjectNode());

        PropertyDefinition<Object> missingProperty = PropertyDefinition.define("absent.value", Long.class);
        assertFalse(supplier.getProperty(missingProperty).isPresent());
    }

    @Test
    public void testRegisterWithDefaultSettings() {
        when(centralDogma.normalizeRevision(PROJECT_NAME, REPOSITORY_NAME, Revision.HEAD)).thenReturn(
                CompletableFuture.completedFuture(Revision.HEAD)
        );
        when(centralDogma.listFiles(PROJECT_NAME, REPOSITORY_NAME, Revision.HEAD, FILENAME)).thenReturn(
                CompletableFuture.completedFuture(Collections.emptyMap())
        );
        when(centralDogma.push(
                eq(PROJECT_NAME),
                eq(REPOSITORY_NAME),
                eq(Revision.HEAD),
                any(String.class),
                eq(Change.ofJsonUpsert(FILENAME, defaultPropertiesAsJsonNode())))
        ).thenReturn(
                CompletableFuture.completedFuture(
                        new PushResult(Revision.HEAD, 1)
                )
        );

        CentralDogmaPropertySupplier.register(centralDogma, PROJECT_NAME, REPOSITORY_NAME, FILENAME);
        verify(centralDogma, times(1)).push(
                eq(PROJECT_NAME),
                eq(REPOSITORY_NAME),
                eq(Revision.HEAD),
                any(String.class),
                eq(Change.ofJsonUpsert(FILENAME, defaultPropertiesAsJsonNode()))
        );

    }

    @Test
    public void testRegisterWithCustomizedSettings() {
        final int settingForPartitionConcurrency = 188;
        final int settingForMaxPendingRecords = 121212;
        final int whenCentralDogmaPushed = 111111;

        List<Property<?>> listPropertiesProvidedByUser = Arrays.asList(
                Property.ofStatic(
                        ProcessorProperties.CONFIG_PARTITION_CONCURRENCY,
                        settingForPartitionConcurrency),
                Property.ofStatic(
                        ProcessorProperties.CONFIG_MAX_PENDING_RECORDS,
                        settingForMaxPendingRecords
                )
        );
        final PropertySupplier supplier = StaticPropertySupplier.of(listPropertiesProvidedByUser);

        final List<Property<?>> listPropertiesForVerifyingMock = ProcessorProperties
                .defaultProperties()
                .stream()
                .map(defaultProperty -> {
                    Optional<? extends Property<?>> prop = supplier.getProperty(defaultProperty.definition());
                    if (prop.isPresent()) {
                        return prop.get();
                    } else {
                        return defaultProperty;
                    }
                }).collect(Collectors.toList());

        final JsonNode jsonNodeProperties = CentralDogmaPropertySupplier
                .convertPropertyListToJsonNode(listPropertiesForVerifyingMock);

        when(centralDogma.normalizeRevision(PROJECT_NAME, REPOSITORY_NAME, Revision.HEAD)).thenReturn(
                CompletableFuture.completedFuture(Revision.HEAD)
        );
        when(centralDogma.listFiles(PROJECT_NAME, REPOSITORY_NAME, Revision.HEAD, FILENAME)).thenReturn(
                CompletableFuture.completedFuture(Collections.emptyMap())
        );

        when(centralDogma.push(
                eq(PROJECT_NAME),
                eq(REPOSITORY_NAME),
                eq(Revision.HEAD),
                any(String.class),
                eq(Change.ofJsonUpsert(FILENAME, jsonNodeProperties)))
        ).thenReturn(
                CompletableFuture.completedFuture(
                        new PushResult(Revision.HEAD, whenCentralDogmaPushed)
                )
        );

        CentralDogmaPropertySupplier.register(centralDogma, PROJECT_NAME, REPOSITORY_NAME, FILENAME, supplier);

        verify(centralDogma, times(1)).push(
                eq(PROJECT_NAME),
                eq(REPOSITORY_NAME),
                eq(Revision.HEAD),
                any(String.class),
                eq(Change.ofJsonUpsert(FILENAME, jsonNodeProperties))
        );
    }

    private static JsonNode defaultPropertiesAsJsonNode() {
        return CentralDogmaPropertySupplier.convertPropertyListToJsonNode(
                ProcessorProperties.defaultProperties());
    }
}
