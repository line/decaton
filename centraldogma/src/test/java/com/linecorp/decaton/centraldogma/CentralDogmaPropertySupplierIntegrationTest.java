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

import static com.linecorp.decaton.processor.runtime.ProcessorProperties.CONFIG_PARTITION_CONCURRENCY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
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

import org.junit.Rule;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;

import com.linecorp.centraldogma.client.CentralDogma;
import com.linecorp.centraldogma.common.Change;
import com.linecorp.centraldogma.common.Entry;
import com.linecorp.centraldogma.common.Query;
import com.linecorp.centraldogma.common.Revision;
import com.linecorp.centraldogma.internal.Jackson;
import com.linecorp.centraldogma.testing.junit4.CentralDogmaRule;
import com.linecorp.decaton.processor.runtime.Property;

public class CentralDogmaPropertySupplierIntegrationTest {

    @Rule
    public CentralDogmaRule centralDogmaRule = new CentralDogmaRule();

    private static final String PROJECT_NAME = "unit-test";
    private static final String REPOSITORY_NAME = "repo";
    private static final String FILENAME = "/subscription.json";

    @Test(timeout = 50000)
    public void testCDIntegration() throws InterruptedException {
        CentralDogma client = centralDogmaRule.client();

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
        client.createRepository(PROJECT_NAME, REPOSITORY_NAME).join();
        client.push(PROJECT_NAME, REPOSITORY_NAME, Revision.HEAD, "summary",
                    Change.ofJsonUpsert(FILENAME, ORIGINAL)).join();

        CentralDogmaPropertySupplier supplier = new CentralDogmaPropertySupplier(
                client, PROJECT_NAME, REPOSITORY_NAME, FILENAME);

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

        client.push(PROJECT_NAME, REPOSITORY_NAME, Revision.HEAD, "summary",
                    Change.ofJsonPatch(FILENAME, ORIGINAL, UPDATED)).join();

        latch.await();
        assertEquals(20, prop.value().intValue());
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
