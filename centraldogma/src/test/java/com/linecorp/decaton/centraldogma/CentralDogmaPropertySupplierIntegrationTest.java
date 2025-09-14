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
import static com.linecorp.decaton.processor.runtime.ProcessorProperties.CONFIG_PARTITION_CONCURRENCY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.linecorp.decaton.processor.runtime.PropertyDefinition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

import com.fasterxml.jackson.databind.JsonNode;

import com.linecorp.armeria.client.WebClientBuilder;
import com.linecorp.armeria.client.retry.RetryRule;
import com.linecorp.armeria.client.retry.RetryingClient;
import com.linecorp.centraldogma.client.CentralDogma;
import com.linecorp.centraldogma.client.CentralDogmaRepository;
import com.linecorp.centraldogma.common.Change;
import com.linecorp.centraldogma.common.Entry;
import com.linecorp.centraldogma.common.Query;
import com.linecorp.centraldogma.common.Revision;
import com.linecorp.centraldogma.internal.Jackson;
import com.linecorp.centraldogma.testing.junit.CentralDogmaExtension;
import com.linecorp.decaton.processor.runtime.ProcessorProperties;
import com.linecorp.decaton.processor.runtime.Property;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class CentralDogmaPropertySupplierIntegrationTest {
    @RegisterExtension
    final CentralDogmaExtension extension = new CentralDogmaExtension() {
        @Override
        protected boolean runForEachTest() {
            return true;
        }

        @Override
        protected void configureHttpClient(WebClientBuilder builder) {
            builder.decorator(RetryingClient.builder(RetryRule.onUnprocessed())
                                            .maxTotalAttempts(3)
                                            .newDecorator());
        }
    };

    private static final String PROJECT_NAME = "unit-test";
    private static final String REPOSITORY_NAME = "repo";

    private JsonNode defaultProperties() {
        return CentralDogmaPropertySupplier.convertPropertyListToJsonNode(
                ProcessorProperties.defaultProperties());
    }

    @Test
    @Timeout(50)
    public void testCDIntegrationJson() throws InterruptedException {
        final String FILENAME = "/subscription.json";
        CentralDogma client = extension.client();

        final String ORIGINAL =
                "{\n"
                + "  \"decaton.partition.concurrency\": 10,\n"
                + "  \"decaton.ignore.keys\": [\n"
                + "    \"123456\",\n"
                + "    \"79797979\"\n"
                + "  ],\n"
                + "  \"decaton.processing.rate.per.partition\": 50\n"
                + "}\n";

        client.createProject(PROJECT_NAME).join();
        CentralDogmaRepository centralDogmaRepository = client.createRepository(PROJECT_NAME, REPOSITORY_NAME).join();
        centralDogmaRepository
              .commit("summary", Change.ofJsonUpsert(FILENAME, ORIGINAL))
              .push()
              .join();

        CentralDogmaPropertySupplier supplier = new CentralDogmaPropertySupplier(centralDogmaRepository, FILENAME);

        Property<Integer> prop = supplier.getProperty(CONFIG_PARTITION_CONCURRENCY).get();

        assertEquals(10, prop.value().intValue());

        final String UPDATED =
                "{\n"
                + "  \"decaton.partition.concurrency\": 20,\n"
                + "  \"decaton.ignore.keys\": [\n"
                + "    \"123456\",\n"
                + "    \"79797979\"\n"
                + "  ],\n"
                + "  \"decaton.processing.rate.per.partition\": 50\n"
                + "}\n";

        CountDownLatch latch = new CountDownLatch(2);
        prop.listen((o, n) -> latch.countDown());

        centralDogmaRepository
              .commit("summary", Change.ofJsonPatch(FILENAME, ORIGINAL, UPDATED))
              .push()
              .join();

        latch.await();
        assertEquals(20, prop.value().intValue());

        assertEquals(20, IntStream
                .range(0, 10000)
                .mapToObj(i -> CONFIG_PARTITION_CONCURRENCY)
                .map(supplier::getProperty)
                .reduce((l, r) -> {
                    assertSame(l.get(), r.get());
                    return l;
                }).get().get().value().intValue());
    }

    @Test
    @Timeout(50)
    void testCDIntegrationYaml() throws Exception {
        final String FILE = "/subscription.yaml";
        CentralDogma client = extension.client();

        client.createProject(PROJECT_NAME).join();
        CentralDogmaRepository repo =
                client.createRepository(PROJECT_NAME, REPOSITORY_NAME).join();

        final String ORIGINAL_YAML =
                        "# processor properties\n"
                        + "decaton.partition.concurrency: 10\n"
                        + "\n"
                        + "# keys to ignore\n"
                        + "decaton.ignore.keys:\n"
                        + "  - \"123456\" # hi\n"
                        + "  - \"79797979\" # hello\n"
                        + "\n"
                        + "decaton.processing.rate.per.partition: 50\n";

        repo.commit("init-yaml", Change.ofTextUpsert(FILE, ORIGINAL_YAML))
                .push().join();

        CentralDogmaPropertySupplier supplier = new CentralDogmaPropertySupplier(repo, FILE);

        Property<Integer> concurrency =
                supplier.getProperty(CONFIG_PARTITION_CONCURRENCY).get();
        Property<java.util.List<String>> ignoreKeys =
                supplier.getProperty(ProcessorProperties.CONFIG_IGNORE_KEYS).get();

        assertEquals(10, concurrency.value());
        assertEquals(java.util.Arrays.asList("123456", "79797979"), ignoreKeys.value());

        CountDownLatch latch = new CountDownLatch(2);
        concurrency.listen((o, n) -> latch.countDown());

        AtomicBoolean firstCall = new AtomicBoolean(true);

        ignoreKeys.listen((oldVal, newVal) -> {
            // null to list is allowed
            if (firstCall.getAndSet(false)) {
                return;
            }
            fail("ignoreKeys should not be updated after the first call");
        });

        final String UPDATED_YAML =
                        "# processor properties\n"
                        + "decaton.partition.concurrency: 20\n" // This is changed
                        + "\n"
                        + "# keys to ignore\n"
                        + "decaton.ignore.keys:\n"
                        + "  - \"123456\" # hi\n"
                        + "  - \"79797979\" # hello\n"
                        + "\n"
                        + "decaton.processing.rate.per.partition: 50\n";

        repo.commit("patch-yaml", Change.ofTextPatch(FILE, ORIGINAL_YAML, UPDATED_YAML))
                .push().join();

        latch.await();
        assertEquals(20, concurrency.value());
        assertEquals(java.util.Arrays.asList("123456", "79797979"), ignoreKeys.value());

        assertEquals(20, IntStream
                .range(0, 10_000)
                .mapToObj(i -> CONFIG_PARTITION_CONCURRENCY)
                .map(supplier::getProperty)
                .reduce((l, r) -> {
                    assertSame(l.get(), r.get());
                    return l;
                }).get().get().value().intValue());
    }

    @Test
    @Timeout(50)
    public void testCDIntegrationDynamicPropertyJson() throws InterruptedException {
        final String FILE = "/subscription.json";
        CentralDogma client = extension.client();

        client.createProject(PROJECT_NAME).join();
        CentralDogmaRepository repo =
                client.createRepository(PROJECT_NAME, REPOSITORY_NAME).join();

        final String topic = "orders";
        final String dynamicName =
                "decaton.shaping.topic.processing.rate.per.partition." + topic;
        assertFalse(ProcessorProperties.defaultProperties().stream()
                .anyMatch(p -> p.definition().name().equals(dynamicName)));

        final String ORIGINAL =
                "{\n"
                        + "  \"" + dynamicName + "\": 7,\n"
                        + "  \"decaton.partition.concurrency\": 10\n"
                        + "}\n";

        repo.commit("init-json", Change.ofJsonUpsert(FILE, ORIGINAL)).push().join();

        CentralDogmaPropertySupplier supplier = new CentralDogmaPropertySupplier(repo, FILE);

        PropertyDefinition<Integer> DYNAMIC_INT =
                PropertyDefinition.define(dynamicName, Integer.class, 0, v -> v instanceof Integer);

        Property<Integer> prop = supplier.getProperty(DYNAMIC_INT).get();
        assertEquals(7, prop.value().intValue());

        final String UPDATED =
                "{\n"
                        + "  \"" + dynamicName + "\": 11,\n" // This is changed
                        + "  \"decaton.partition.concurrency\": 10\n"
                        + "}\n";

        CountDownLatch latch = new CountDownLatch(2);
        prop.listen((oldV, newV) -> latch.countDown());

        repo.commit("patch-json", Change.ofJsonPatch(FILE, ORIGINAL, UPDATED)).push().join();

        latch.await();
        assertEquals(11, prop.value().intValue());
    }

    @Test
    @Timeout(50)
    public void testCDIntegrationDynamicPropertyYaml() throws Exception {
        final String FILE = "/subscription.yaml";
        CentralDogma client = extension.client();

        client.createProject(PROJECT_NAME).join();
        CentralDogmaRepository repo =
                client.createRepository(PROJECT_NAME, REPOSITORY_NAME).join();

        final String topic = "payments";
        final String dynamicName =
                "decaton.shaping.topic.processing.rate.per.partition." + topic;
        assertFalse(ProcessorProperties.defaultProperties().stream()
                .anyMatch(p -> p.definition().name().equals(dynamicName)));

        final String ORIGINAL_YAML =
                "decaton.partition.concurrency: 10\n"
                        + dynamicName + ": 3\n";

        repo.commit("init-yaml", Change.ofTextUpsert(FILE, ORIGINAL_YAML))
                .push().join();

        CentralDogmaPropertySupplier supplier = new CentralDogmaPropertySupplier(repo, FILE);

        PropertyDefinition<Integer> DYNAMIC_INT =
                PropertyDefinition.define(dynamicName, Integer.class, 0, v -> v instanceof Integer);

        Property<Integer> prop = supplier.getProperty(DYNAMIC_INT).get();
        assertEquals(3, prop.value().intValue());

        final String UPDATED_YAML =
                "decaton.partition.concurrency: 10\n"
                        + dynamicName + ": 9\n"; // This is changed

        CountDownLatch latch = new CountDownLatch(2);
        prop.listen((o, n) -> latch.countDown());

        repo.commit("patch-yaml", Change.ofTextPatch(FILE, ORIGINAL_YAML, UPDATED_YAML))
                .push().join();

        latch.await();
        assertEquals(9, prop.value().intValue());
    }

    @Test
    @Timeout(10)
    public void testCDRegisterSuccessJson() {
        final String FILENAME = "/subscription.json";
        CentralDogma client = extension.client();
        client.createProject(PROJECT_NAME).join();
        CentralDogmaRepository centralDogmaRepository = client.createRepository(PROJECT_NAME, REPOSITORY_NAME).join();

        CentralDogmaPropertySupplier.register(centralDogmaRepository, FILENAME);
        Entry<JsonNode> prop = centralDogmaRepository.file(Query.ofJson(FILENAME)).get().join();

        JsonNode expected = defaultProperties();
        JsonNode actual = prop.content();
        assertEquals(expected.toString(), actual.toString(),
                () -> "\nexpected: " + expected.toPrettyString()
                        + "\nactual: " + actual.toPrettyString());
    }

    @Test
    @Timeout(10)
    public void testCDRegisterSuccessYaml() throws Exception {
        String yamlFile = "/subscription.yaml";
        CentralDogma client = extension.client();
        client.createProject(PROJECT_NAME).join();
        CentralDogmaRepository centralDogmaRepository = client.createRepository(PROJECT_NAME, REPOSITORY_NAME).join();

        CentralDogmaPropertySupplier.register(centralDogmaRepository, yamlFile);

        String actualText = centralDogmaRepository.file(Query.ofText(yamlFile)).get().join().content();

        ObjectMapper yaml = new ObjectMapper(new YAMLFactory());
        JsonNode actual = yaml.readTree(actualText);
        JsonNode expected = defaultProperties();

        assertEquals(expected.toString(), actual.toString(),
                () -> "\nexpected: " + expected.toPrettyString()
                        + "\nactual: " + actual.toPrettyString());

        assertFalse(actualText.startsWith("---"), "YAML should not include doc start marker");
        assertFalse(actualText.trim().startsWith("{"), "YAML should not be JSON text");
    }

    @Test
    @Timeout(15)
    public void testCDRegisterConflictJson() throws Exception {
        final String FILENAME = "/subscription.json";
        CountDownLatch userAIsRunning = new CountDownLatch(1);
        CountDownLatch userBIsRunning = new CountDownLatch(1);

        CentralDogma client = extension.client();
        client.createProject(PROJECT_NAME).join();
        CentralDogmaRepository userB = client.createRepository(PROJECT_NAME, REPOSITORY_NAME).join();
        CentralDogmaRepository userA = spy(client.forRepo(PROJECT_NAME, REPOSITORY_NAME));
        JsonNode userBPush = Jackson.readTree("{\"foo\": \"bar\"}");

        doAnswer(i -> {
            userAIsRunning.countDown();
            userBIsRunning.await();
            return i.callRealMethod();
        }).when(userA)
          .commit(any(), eq(Change.ofJsonUpsert(FILENAME, defaultProperties())));

        ExecutorService service = Executors.newFixedThreadPool(2);
        service.submit(() -> CentralDogmaPropertySupplier.register(userA, FILENAME));
        service.submit(() -> {
            try {
                userAIsRunning.await();
                userB.commit("test", Change.ofJsonUpsert(FILENAME, userBPush))
                     .push()
                     .join();
                userBIsRunning.countDown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        });
        service.shutdown();
        service.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);

        Entry<JsonNode> prop = userA.file(Query.ofJson(FILENAME))
                                    .get()
                                    .join();

        assertEquals(userBPush, prop.content());
    }

    @Test
    @Timeout(15)
    void testCDRegisterConflictYaml() throws Exception {
        final String FILE = "/subscription.yaml";
        CountDownLatch userAIsRunning = new CountDownLatch(1);
        CountDownLatch userBIsRunning = new CountDownLatch(1);

        CentralDogma client = extension.client();
        client.createProject(PROJECT_NAME).join();

        CentralDogmaRepository userB = client.createRepository(PROJECT_NAME, REPOSITORY_NAME).join();
        CentralDogmaRepository userA = spy(client.forRepo(PROJECT_NAME, REPOSITORY_NAME));

        final String userBYaml =
                "# pushed by user‑B (should win the race)\n"
                        + "foo: bar\n";

        JsonNode userBYamlAsJsonNode = Jackson.readTree("{\"foo\":\"bar\"}");

        String defaultYaml = new ObjectMapper(new YAMLFactory().disable(WRITE_DOC_START_MARKER))
                .writeValueAsString(defaultProperties());

        doAnswer(inv -> {
            userAIsRunning.countDown();
            userBIsRunning.await();
            return inv.callRealMethod();
        }).when(userA)
                .commit(any(), eq(Change.ofTextUpsert(FILE, defaultYaml)));

        ExecutorService svc = Executors.newFixedThreadPool(2);
        svc.submit(() -> CentralDogmaPropertySupplier.register(userA, FILE));
        svc.submit(() -> {
            try {
                userAIsRunning.await();
                userB.commit("userB‑push", Change.ofTextUpsert(FILE, userBYaml))
                        .push().join();
                userBIsRunning.countDown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        });

        svc.shutdown();
        svc.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);

        Entry<String> entry = userA.file(Query.ofText(FILE)).get().join();

        assertEquals(userBYaml, entry.content());

        JsonNode actual = new ObjectMapper(new YAMLFactory())
                .readTree(entry.content());
        assertEquals(userBYamlAsJsonNode, actual);
    }


    interface FormatCase {
        String file();

        Change<?> upsert(String body);

        String emptyBody();
    }

    private static final FormatCase JSON = new FormatCase() {
        public String file() {
            return "/subscription.json";
        }

        public Change<?> upsert(String body) {
            return Change.ofJsonUpsert(file(), body);
        }

        public String emptyBody() {
            return "{}";
        }

        @Override
        public String toString() {
            return "JSON";
        }
    };

    private static final FormatCase YAML = new FormatCase() {
        public String file() {
            return "/subscription.yaml";
        }

        public Change<?> upsert(String body) {
            return Change.ofTextUpsert(file(), body);
        }

        public String emptyBody() {
            return "";
        }

        @Override
        public String toString() {
            return "YAML";
        }
    };

    static Stream<FormatCase> formats() {
        return Stream.of(JSON, YAML);
    }

    @ParameterizedTest()
    @MethodSource("formats")
    @Timeout(15)
    void testCDRegisterTimeout(FormatCase testCase) {
        CentralDogma client = extension.client();
        client.createProject(PROJECT_NAME).join();
        CentralDogmaRepository centralDogmaRepository = spy(client.createRepository(PROJECT_NAME, REPOSITORY_NAME).join());

        doReturn(CompletableFuture.completedFuture(new Revision(1)))
                .when(centralDogmaRepository)
                .normalize(any());

        CentralDogmaPropertySupplier.register(centralDogmaRepository, testCase.file());

        assertThrows(RuntimeException.class, () -> {
            CentralDogmaPropertySupplier.register(centralDogmaRepository, testCase.file());
        });
    }

    @ParameterizedTest()
    @MethodSource("formats")
    void testCDRegisterNonExistentProject(FormatCase testCase) {
        assertThrows(RuntimeException.class, () -> {
            CentralDogmaPropertySupplier.register(extension.client(),
                    "non-existent-project", REPOSITORY_NAME, testCase.file());
        });
    }

    @ParameterizedTest()
    @MethodSource("formats")
    void testFileExist(FormatCase testCase) {
        CentralDogma client = extension.client();
        client.createProject(PROJECT_NAME).join();
        CentralDogmaRepository centralDogmaRepository = client.createRepository(PROJECT_NAME, REPOSITORY_NAME).join();

        centralDogmaRepository
                .commit("test", testCase.upsert(testCase.emptyBody()))
                .push()
                .join();
        assertTrue(CentralDogmaPropertySupplier
                .fileExists(centralDogmaRepository, testCase.file(), Revision.HEAD));
    }

    @ParameterizedTest()
    @MethodSource("formats")
    void testFileNonExistent(FormatCase testCase) {
        CentralDogma client = extension.client();
        client.createProject(PROJECT_NAME).join();
        CentralDogmaRepository centralDogmaRepository = client.createRepository(PROJECT_NAME, REPOSITORY_NAME).join();
        assertFalse(CentralDogmaPropertySupplier
                .fileExists(centralDogmaRepository, testCase.file(), Revision.HEAD));
    }
}
