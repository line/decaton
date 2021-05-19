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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

import com.linecorp.centraldogma.client.CentralDogma;
import com.linecorp.centraldogma.client.Watcher;
import com.linecorp.centraldogma.common.Change;
import com.linecorp.centraldogma.common.Entry;
import com.linecorp.centraldogma.common.Query;
import com.linecorp.centraldogma.common.Revision;
import com.linecorp.centraldogma.internal.Jackson;
import com.linecorp.centraldogma.testing.junit4.CentralDogmaRule;
import com.linecorp.decaton.processor.runtime.DynamicProperty;
import com.linecorp.decaton.processor.runtime.Property;
import com.linecorp.decaton.processor.runtime.PropertyDefinition;

public class CentralDogmaPropertySupplierTest {
    @Rule
    public MockitoRule rule = MockitoJUnit.rule();

    @Rule
    public CentralDogmaRule centralDogmaRule = new CentralDogmaRule();

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

    private CentralDogmaPropertySupplier supplier;

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Before
    public void setUp() {
        Watcher<Map> rootWatcher = mock(Watcher.class);
        doAnswer(invocation -> {
            ((Consumer<Map>) invocation.getArgument(0)).accept(
                    Stream.of(LONG_PROPERTY, LIST_PROPERTY)
                          .collect(Collectors.toMap(PropertyDefinition::name, p -> "")));
            return null;
        }).when(rootWatcher).watch((Consumer<? super Map>) any());

        doReturn(rootWatcher)
                .when(centralDogma)
                .fileWatcher(eq(PROJECT_NAME), eq(REPOSITORY_NAME),
                             eq(Query.ofJsonPath(FILENAME, "$")), any());

        supplier = new CentralDogmaPropertySupplier(centralDogma, PROJECT_NAME, REPOSITORY_NAME,
                                                    FILENAME);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testWatcherSetup() {
        Watcher<JsonNode> longPropertyWatcher = mock(Watcher.class);
        Watcher<JsonNode> listPropertyWatcher = mock(Watcher.class);

        when(centralDogma.fileWatcher(eq(PROJECT_NAME), eq(REPOSITORY_NAME), (Query<JsonNode>) any()))
                .thenReturn(longPropertyWatcher)
                .thenReturn(listPropertyWatcher)
                .thenReturn(null);

        supplier.getProperty(LONG_PROPERTY);

        verify(centralDogma, times(1)).fileWatcher(eq(PROJECT_NAME), eq(REPOSITORY_NAME), any());
        verify(longPropertyWatcher, times(1)).watch(any(Consumer.class));
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
        verify(prop, times(1)).checkingSet(Long.valueOf(10L));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testGetProperty() {
        Watcher<JsonNode> longPropertyWatcher = mock(Watcher.class);
        doAnswer(invocation -> {
            ((Consumer<JsonNode>) invocation.getArgument(0)).accept(
                    objectMapper.getNodeFactory().numberNode(100L));
            return null;
        }).when(longPropertyWatcher).watch((Consumer<JsonNode>) any());

        when(centralDogma.fileWatcher(eq(PROJECT_NAME), eq(REPOSITORY_NAME), (Query<JsonNode>) any()))
                .thenReturn(longPropertyWatcher)
                .thenReturn(null);

        Property<Long> prop = supplier.getProperty(LONG_PROPERTY).get();

        assertEquals(LONG_PROPERTY, prop.definition());
        assertEquals(100L, prop.value().longValue());
    }

    @Test(timeout = 5000)
    public void testGetPropertyAbsentName() {
        assertFalse(supplier.getProperty(PropertyDefinition.define("absent.value", Long.class)).isPresent());
    }

    @Test(timeout = 50000)
    public void testCDIntegration() throws InterruptedException {
        CentralDogma client = centralDogmaRule.client();

        final String ORIGINAL =
                "{\n"
                + "  \"num.property\": 10,\n"
                + "  \"ignore.keys\": [\n"
                + "    \"123456\",\n"
                + "    \"79797979\"\n"
                + "  ],\n"
                + "  \"partition.processing.rate\": 50\n"
                + "}\n";

        client.createProject(PROJECT_NAME).join();
        client.createRepository(PROJECT_NAME, REPOSITORY_NAME).join();
        client.push(PROJECT_NAME, REPOSITORY_NAME, Revision.HEAD, "summary",
                    Change.ofTextUpsert(FILENAME, ORIGINAL)).join();

        CentralDogmaPropertySupplier supplier = new CentralDogmaPropertySupplier(
                client, PROJECT_NAME, REPOSITORY_NAME, FILENAME);

        Property<Long> prop = supplier.getProperty(LONG_PROPERTY).get();

        assertEquals(10L, prop.value().longValue());

        final String UPDATED =
                "{\n"
                + "  \"num.property\": 20,\n"
                + "  \"ignore.keys\": [\n"
                + "    \"123456\",\n"
                + "    \"79797979\"\n"
                + "  ],\n"
                + "  \"partition.processing.rate\": 50\n"
                + "}\n";

        CountDownLatch latch = new CountDownLatch(2);
        prop.listen((o, n) -> latch.countDown());

        client.push(PROJECT_NAME, REPOSITORY_NAME, Revision.HEAD, "summary",
                    Change.ofTextPatch(FILENAME, ORIGINAL, UPDATED)).join();

        latch.await();
        assertEquals(20L, prop.value().longValue());
    }

    @Test
    public void testFileExist() {
        CentralDogma client = centralDogmaRule.client();
        client.createProject(PROJECT_NAME).join();
        client.createRepository(PROJECT_NAME, REPOSITORY_NAME).join();
        client.push(PROJECT_NAME, REPOSITORY_NAME, Revision.HEAD, "test",
                    Change.ofJsonUpsert(FILENAME, "{}")).join();
        assertTrue(CentralDogmaPropertySupplier
                           .fileExists(client, PROJECT_NAME, REPOSITORY_NAME, FILENAME, Revision.HEAD));
    }

    @Test
    public void testFileNonExistent() {
        CentralDogma client = centralDogmaRule.client();
        client.createProject(PROJECT_NAME).join();
        client.createRepository(PROJECT_NAME, REPOSITORY_NAME).join();
        assertFalse(CentralDogmaPropertySupplier
                            .fileExists(client, PROJECT_NAME, REPOSITORY_NAME, FILENAME, Revision.HEAD));
    }

    @Test(timeout = 10000)
    public void testCDRegisterSuccess() {
        CentralDogma client = centralDogmaRule.client();
        client.createProject(PROJECT_NAME).join();
        client.createRepository(PROJECT_NAME, REPOSITORY_NAME).join();

        CentralDogmaPropertySupplier.register(client, PROJECT_NAME, REPOSITORY_NAME, FILENAME);
        Entry<JsonNode> prop = client.getFile(PROJECT_NAME, REPOSITORY_NAME,
                                              Revision.HEAD, Query.ofJson(FILENAME)).join();

        assertEquals(CentralDogmaPropertySupplier.defaultProperties().asText(),
                     prop.content().asText());
    }

    @Test(timeout = 10000, expected = RuntimeException.class)
    public void testCDRegisterNonExistentProject() {
        CentralDogmaPropertySupplier.register(centralDogmaRule.client(),
                                              "non-existent-project", REPOSITORY_NAME, FILENAME);
    }

    @Test(timeout = 15000, expected = RuntimeException.class)
    public void testCDRegisterTimeout() {
        CentralDogma client = spy(centralDogmaRule.client());
        client.createProject(PROJECT_NAME).join();
        client.createRepository(PROJECT_NAME, REPOSITORY_NAME).join();

        doReturn(CompletableFuture.completedFuture(new Revision(1)))
                .when(client)
                .normalizeRevision(eq(PROJECT_NAME), eq(REPOSITORY_NAME), any());

        CentralDogmaPropertySupplier.register(client, PROJECT_NAME, REPOSITORY_NAME, FILENAME);

        CentralDogmaPropertySupplier.register(client, PROJECT_NAME, REPOSITORY_NAME, FILENAME);
    }

    @Test(timeout = 15000)
    public void testCDRegisterConflict() throws Exception {
        CountDownLatch userAIsRunning = new CountDownLatch(1);
        CountDownLatch userBIsRunning = new CountDownLatch(1);

        CentralDogma userA = spy(centralDogmaRule.client());
        CentralDogma userB = centralDogmaRule.client();
        JsonNode userBPush = Jackson.readTree("{\"foo\": \"bar\"}");

        userA.createProject(PROJECT_NAME).join();
        userA.createRepository(PROJECT_NAME, REPOSITORY_NAME).join();

        doAnswer(i -> {
            userAIsRunning.countDown();
            userBIsRunning.await();
            return i.callRealMethod();
        }).when(userA)
          .push(eq(PROJECT_NAME), eq(REPOSITORY_NAME), any(), any(),
                eq(Change.ofJsonUpsert(FILENAME, CentralDogmaPropertySupplier.defaultProperties())));

        ExecutorService service = Executors.newFixedThreadPool(2);
        service.submit(() -> CentralDogmaPropertySupplier
                .register(userA, PROJECT_NAME, REPOSITORY_NAME, FILENAME));
        service.submit(() -> {
            try {
                userAIsRunning.await();
                userB.push(PROJECT_NAME, REPOSITORY_NAME, Revision.HEAD, "test",
                           Change.ofJsonUpsert(FILENAME, userBPush)).join();
                userBIsRunning.countDown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        });
        service.shutdown();
        service.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);

        Entry<JsonNode> prop = userA.getFile(PROJECT_NAME, REPOSITORY_NAME,
                                             Revision.HEAD, Query.ofJson(FILENAME)).join();

        assertEquals(userBPush, prop.content());
    }
}
