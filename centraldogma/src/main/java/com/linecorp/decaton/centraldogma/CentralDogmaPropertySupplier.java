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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import com.linecorp.centraldogma.client.CentralDogma;
import com.linecorp.centraldogma.client.Watcher;
import com.linecorp.centraldogma.common.Change;
import com.linecorp.centraldogma.common.ChangeConflictException;
import com.linecorp.centraldogma.common.EntryType;
import com.linecorp.centraldogma.common.Query;
import com.linecorp.centraldogma.common.Revision;
import com.linecorp.decaton.processor.runtime.DynamicProperty;
import com.linecorp.decaton.processor.runtime.ProcessorProperties;
import com.linecorp.decaton.processor.runtime.Property;
import com.linecorp.decaton.processor.runtime.PropertyDefinition;
import com.linecorp.decaton.processor.runtime.PropertySupplier;

/**
 * A {@link PropertySupplier} implementation with Central Dogma backend.
 *
 * This implementation maps property's {@link PropertyDefinition#name()} as the absolute field name in the file
 * on Central Dogma.
 *
 * An example JSON format would be look like:
 * {@code
 * {
 *     "decaton.partition.concurrency": 10,
 *     "decaton.ignore.keys": [
 *       "123456",
 *       "79797979"
 *     ],
 *     "decaton.processing.rate.per.partition": 50
 * }
 * }
 */
public class CentralDogmaPropertySupplier implements PropertySupplier, AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(CentralDogmaPropertySupplier.class);

    private static final long INITIAL_VALUE_TIMEOUT_SECS = 30;
    private static final long PROPERTY_CREATION_TIMEOUT_MILLIS = 10000;

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final Watcher<JsonNode> rootWatcher;

    /**
     * Creates a new {@link CentralDogmaPropertySupplier}.
     * @param centralDogma a {@link CentralDogma} instance to use to access Central Dogma server.
     * @param projectName the project name where the properties are placed.
     * @param repositoryName the repository name where the properties are placed.
     * @param fileName the name of the file containing properties as top-level fields.
     */
    public CentralDogmaPropertySupplier(CentralDogma centralDogma, String projectName,
                                        String repositoryName, String fileName) {
        rootWatcher = centralDogma.fileWatcher(projectName, repositoryName, Query.ofJsonPath(fileName));
        try {
            rootWatcher.awaitInitialValue(INITIAL_VALUE_TIMEOUT_SECS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    // visible for testing
    Object convertNodeToValue(DynamicProperty<?> prop, JsonNode node) {
        return objectMapper.convertValue(node, prop.definition().runtimeType());
    }

    // visible for testing
    void setValue(DynamicProperty<?> prop, JsonNode valueNode) {
        Object instantValue = convertNodeToValue(prop, valueNode);
        prop.checkingSet(instantValue);
    }

    @Override
    public <T> Optional<Property<T>> getProperty(PropertyDefinition<T> definition) {
        if (!rootWatcher.latestValue().has(definition.name())) {
            return Optional.empty();
        }

        DynamicProperty<T> prop = new DynamicProperty<>(definition);
        Watcher<JsonNode> child = rootWatcher.newChild(jsonNode -> jsonNode.path(definition.name()));
        child.watch(node -> {
            try {
                setValue(prop, node);
            } catch (RuntimeException e) {
                logger.warn("Failed to set value updated from CentralDogma for {}", definition.name(), e);
            }
        });
        try {
            JsonNode node = child.initialValueFuture().join().value(); //doesn't fail since it's a child watcher
            setValue(prop, node);
        } catch (RuntimeException e) {
            logger.warn("Failed to set initial value from CentralDogma for {}", definition.name(), e);
        }

        return Optional.of(prop);
    }

    @Override
    public void close() {
        rootWatcher.close();
    }

    /**
     * Create a default property file if it doesn't exist on Central Dogma and
     * return a {@link CentralDogmaPropertySupplier}.
     * @param centralDogma a {@link CentralDogma} instance to use to access Central Dogma server.
     * @param project the project name where the properties are placed.
     * @param repository the repository name where the properties are placed.
     * @param filename the name of the file containing properties as top-level fields.
     */
    public static CentralDogmaPropertySupplier register(CentralDogma centralDogma, String project,
                                                        String repository, String filename) {
        createPropertyFile(centralDogma, project, repository, filename, ProcessorProperties.defaultProperties());
        return new CentralDogmaPropertySupplier(centralDogma, project, repository, filename);
    }

    /**
     * Create a default property file if it doesn't exist on Central Dogma and
     * return a {@link CentralDogmaPropertySupplier}.
     * @param centralDogma a {@link CentralDogma} instance to use to access Central Dogma server.
     * @param project the project name where the properties are placed.
     * @param repository the repository name where the properties are placed.
     * @param filename the name of the file containing properties as top-level fields.
     * @param supplier user-customized settings for kafka-consumer.
     */
    public static CentralDogmaPropertySupplier register(CentralDogma centralDogma, String project,
                                                        String repository, String filename,
                                                        PropertySupplier supplier) {
        List<Property<?>> properties = ProcessorProperties.defaultProperties().stream().map(defaultProperty -> {
            Optional<? extends Property<?>> prop = supplier.getProperty(defaultProperty.definition());
            if (prop.isPresent()) {
                return prop.get();
            } else {
                return defaultProperty;
            }
        }).collect(Collectors.toList());

        createPropertyFile(centralDogma, project, repository, filename, properties);
        return new CentralDogmaPropertySupplier(centralDogma, project, repository, filename);
    }

    private static void createPropertyFile(CentralDogma centralDogma, String project,
                                           String repository, String fileName,
                                           List<Property<?>> properties) {
        Revision baseRevision = normalizeRevision(centralDogma, project, repository, Revision.HEAD);
        boolean fileExists = fileExists(centralDogma, project, repository, fileName, baseRevision);
        long startedTime = System.currentTimeMillis();
        long remainingTime = remainingTime(PROPERTY_CREATION_TIMEOUT_MILLIS, startedTime);

        JsonNode jsonNodeProperties = convertPropertyListToJsonNode(properties);

        while (!fileExists && remainingTime > 0) {
            try {
                centralDogma.push(project, repository, baseRevision,
                                  String.format("[CentralDogmaPropertySupplier] Property file created: %s",
                                                fileName),
                                  Change.ofJsonUpsert(fileName, jsonNodeProperties))
                            .get(remainingTime, TimeUnit.MILLISECONDS);
                logger.info("New property file registered on Central Dogma: {}/{}/{}",
                            project, repository, fileName);
                fileExists = true;
            } catch (ExecutionException e) {
                if (e.getCause() instanceof ChangeConflictException) {
                    logger.warn(
                            "Failed to push to {}. Someone pushed a commit against current revision. Try again",
                            baseRevision);
                    baseRevision = normalizeRevision(centralDogma, project, repository, Revision.HEAD);
                    fileExists = fileExists(centralDogma, project, repository, fileName, baseRevision);
                } else {
                    logger.error("Failed to push to {}. Unexpected exception happened", baseRevision, e);
                    break;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("Failed to push to {}. Thread interrupted", baseRevision, e);
                break;
            } catch (TimeoutException e) {
                logger.error("Failed to push to {}. Failed to create the property file in time",
                             baseRevision, e);
                break;
            }

            remainingTime = remainingTime(PROPERTY_CREATION_TIMEOUT_MILLIS, startedTime);
        }

        if (!fileExists) {
            throw new RuntimeException("Failed to create the property file in time");
        }
    }

    private static Revision normalizeRevision(CentralDogma centralDogma, String project,
                                              String repository, Revision revision) {
        try {
            return centralDogma.normalizeRevision(project, repository, revision)
                               .get(PROPERTY_CREATION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (TimeoutException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    // visible for testing
    static boolean fileExists(CentralDogma centralDogma, String project,
                              String repository, String filename, Revision revision) {
        try {
            Map<String, EntryType> files = centralDogma
                    .listFiles(project, repository, revision, filename)
                    .get(PROPERTY_CREATION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            return files.containsKey(filename);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (TimeoutException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private static long remainingTime(long totalTime, long startedTime) {
        return totalTime - (System.currentTimeMillis() - startedTime);
    }

    // visible for testing
    static JsonNode convertPropertyListToJsonNode(List<Property<?>> properties) {
        final ObjectNode propertiesObjectNode = objectMapper.createObjectNode();
        properties.forEach(
                property -> {
                    propertiesObjectNode.set(
                            property.definition().name(),
                            objectMapper.valueToTree(property.value())
                    );
                }
        );
        return propertiesObjectNode;
    }
}
