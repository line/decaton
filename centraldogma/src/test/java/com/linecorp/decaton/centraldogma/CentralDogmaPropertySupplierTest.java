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

import static com.fasterxml.jackson.dataformat.yaml.YAMLGenerator.Feature.WRITE_DOC_START_MARKER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import com.linecorp.centraldogma.client.CentralDogmaRepository;
import com.linecorp.centraldogma.client.CommitRequest;
import com.linecorp.centraldogma.client.FilesRequest;
import com.linecorp.centraldogma.client.Watcher;
import com.linecorp.centraldogma.client.WatcherRequest;
import com.linecorp.centraldogma.common.Change;
import com.linecorp.centraldogma.common.PathPattern;
import com.linecorp.centraldogma.common.PushResult;
import com.linecorp.centraldogma.common.Query;
import com.linecorp.centraldogma.common.Revision;
import com.linecorp.decaton.processor.runtime.DynamicProperty;
import com.linecorp.decaton.processor.runtime.ProcessorProperties;
import com.linecorp.decaton.processor.runtime.Property;
import com.linecorp.decaton.processor.runtime.PropertyDefinition;
import com.linecorp.decaton.processor.runtime.PropertySupplier;
import com.linecorp.decaton.processor.runtime.StaticPropertySupplier;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class CentralDogmaPropertySupplierTest {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private static final ObjectMapper YAML_MAPPER = new ObjectMapper(
            new YAMLFactory()
                    .disable(WRITE_DOC_START_MARKER)
    );

    private static final PropertyDefinition<Long> LONG_PROPERTY =
            PropertyDefinition.define("num.property", Long.class, 0L,
                                      v -> v instanceof Long && (Long) v >= 0L);

    private static final PropertyDefinition<List<String>> LIST_PROPERTY =
            PropertyDefinition.define("list.property", List.class, Collections.emptyList(),
                                      PropertyDefinition.checkListElement(String.class));

    @Mock
    private CentralDogmaRepository centralDogmaRepository;

    @Mock
    WatcherRequest<JsonNode> watcherRequest;

    @Mock
    Watcher<JsonNode> rootWatcher;

    private static Stream<Arguments> fileParams() {
        return Stream.of(
                Arguments.of("/subscription.json"),
                Arguments.of("/subscription.yaml")
        );
    }

    @SuppressWarnings("unchecked")
    private CentralDogmaPropertySupplier setup(String fileName) {
        when(centralDogmaRepository.watcher(any(Query.class)))
                .thenReturn(watcherRequest);

        // yaml mode
        if (fileName.endsWith(".yaml")) {
            doReturn(watcherRequest)
                    .when(watcherRequest)
                    .map(any());
        }
        when(watcherRequest.start()).thenReturn(rootWatcher);
        return new CentralDogmaPropertySupplier(centralDogmaRepository, fileName);
    }

    @ParameterizedTest()
    @MethodSource("fileParams")
    public void testWatcherSetup(String fileName) {
        CentralDogmaPropertySupplier supplier = setup(fileName);

        when(rootWatcher.latestValue()).thenReturn(
                objectMapper.createObjectNode().put(LONG_PROPERTY.name(), 123L));

        assertTrue(supplier.getProperty(LONG_PROPERTY).isPresent());
    }

    @ParameterizedTest()
    @MethodSource("fileParams")
    public void testConvertValue(String fileName) {
        CentralDogmaPropertySupplier supplier = setup(fileName);

        JsonNodeFactory factory = objectMapper.getNodeFactory();

        Object convertedLong = supplier.convertNodeToValue(
                new DynamicProperty<>(LONG_PROPERTY), factory.numberNode(10L));

        assertSame(Long.class, convertedLong.getClass());
        assertEquals(10L, convertedLong);

        Object convertedList = supplier.convertNodeToValue(new DynamicProperty<>(LIST_PROPERTY),
                                                           factory.arrayNode().add("foo").add("bar"));
        assertEquals(Arrays.asList("foo", "bar"), convertedList);
    }

    @ParameterizedTest()
    @MethodSource("fileParams")
    public void testSetValue(String fileName) {
        CentralDogmaPropertySupplier supplier = setup(fileName);

        JsonNodeFactory factory = objectMapper.getNodeFactory();

        DynamicProperty<Long> prop = spy(new DynamicProperty<>(LONG_PROPERTY));
        supplier.setValue(prop, factory.numberNode(10L));
        verify(prop).checkingSet(10L);
    }

    @ParameterizedTest()
    @MethodSource("fileParams")
    public void testSetNullValue(String fileName) {
        CentralDogmaPropertySupplier supplier = setup(fileName);
        JsonNodeFactory factory = objectMapper.getNodeFactory();

        DynamicProperty<Long> prop = spy(new DynamicProperty<>(LONG_PROPERTY));
        supplier.setValue(prop, factory.nullNode());
        verify(prop).checkingSet(LONG_PROPERTY.defaultValue());
    }

    @ParameterizedTest()
    @MethodSource("fileParams")
    public void testGetPropertyAbsentName(String fileName) {
        CentralDogmaPropertySupplier supplier = setup(fileName);

        when(rootWatcher.latestValue()).thenReturn(objectMapper.createObjectNode());

        PropertyDefinition<Object> missingProperty = PropertyDefinition.define("absent.value", Long.class);
        assertFalse(supplier.getProperty(missingProperty).isPresent());
    }

    private Change<?> expectedChange(String fileName, JsonNode node) throws Exception {
        if (fileName.endsWith(".yaml") || fileName.endsWith(".yml")) {
            return Change.ofTextUpsert(fileName, YAML_MAPPER.writeValueAsString(node));
        } else {
            return Change.ofJsonUpsert(fileName, node);
        }
    }

    @ParameterizedTest()
    @MethodSource("fileParams")
    public void testRegisterWithDefaultSettings(String fileName) throws Exception {
        setup(fileName);

        when(centralDogmaRepository.normalize(Revision.HEAD))
                .thenReturn(CompletableFuture.completedFuture(Revision.HEAD));

        final FilesRequest filesRequest = mock(FilesRequest.class);
        when(centralDogmaRepository.file(any(PathPattern.class))).thenReturn(filesRequest);
        when(filesRequest.list(Revision.HEAD)).thenReturn(CompletableFuture.completedFuture(Collections.emptyMap()));

        final CommitRequest commitRequest = mock(CommitRequest.class);
        final Change<?> upsert = expectedChange(fileName, defaultPropertiesAsJsonNode());
        when(centralDogmaRepository.commit(anyString(), eq(upsert))).thenReturn(commitRequest);
        when(commitRequest.push(Revision.HEAD)).thenReturn(CompletableFuture.completedFuture(new PushResult(Revision.HEAD, 1)));


        CentralDogmaPropertySupplier.register(centralDogmaRepository, fileName);
        verify(centralDogmaRepository).commit(
                anyString(),
                eq(upsert)
        );
    }

    @ParameterizedTest()
    @MethodSource("fileParams")
    public void testRegisterWithCustomizedSettings(String fileName) throws Exception {
        setup(fileName);

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

        when(centralDogmaRepository.normalize(Revision.HEAD))
                .thenReturn(CompletableFuture.completedFuture(Revision.HEAD));

        final FilesRequest filesRequest = mock(FilesRequest.class);
        when(centralDogmaRepository.file(any(PathPattern.class))).thenReturn(filesRequest);
        when(filesRequest.list(Revision.HEAD)).thenReturn(CompletableFuture.completedFuture(Collections.emptyMap()));

        final Change<?> upsert = expectedChange(fileName, jsonNodeProperties);
        final CommitRequest commitRequest = mock(CommitRequest.class);
        when(centralDogmaRepository.commit(anyString(), eq(upsert))).thenReturn(commitRequest);
        when(commitRequest.push(Revision.HEAD)).thenReturn(CompletableFuture.completedFuture(new PushResult(Revision.HEAD, whenCentralDogmaPushed)));

        CentralDogmaPropertySupplier.register(centralDogmaRepository, fileName, supplier);

        verify(centralDogmaRepository).commit(
                anyString(),
                eq(upsert)
        );
    }

    private static JsonNode defaultPropertiesAsJsonNode() {
        return CentralDogmaPropertySupplier.convertPropertyListToJsonNode(
                ProcessorProperties.defaultProperties());
    }
}
