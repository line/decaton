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

import java.util.List;
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
import com.linecorp.centraldogma.client.CentralDogmaRepository;
import com.linecorp.centraldogma.client.Watcher;
import com.linecorp.centraldogma.common.Change;
import com.linecorp.centraldogma.common.ChangeConflictException;
import com.linecorp.centraldogma.common.PathPattern;
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
        this(centralDogma.forRepo(projectName, repositoryName), fileName);
    }

    /**
     * Creates a new {@link CentralDogmaPropertySupplier}.
     * @param centralDogmaRepository {@link CentralDogmaRepository} instance that points to a particular Central Dogma repository.
     * @param fileName the name of the file containing properties as top-level fields.
     */
    public CentralDogmaPropertySupplier(CentralDogmaRepository centralDogmaRepository, String fileName) {
        rootWatcher = centralDogmaRepository.watcher(Query.ofJsonPath(fileName)).start();
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
        final CentralDogmaRepository centralDogmaRepository = centralDogma.forRepo(project, repository);
        createPropertyFile(centralDogmaRepository, filename, ProcessorProperties.defaultProperties());
        return new CentralDogmaPropertySupplier(centralDogmaRepository, filename);
    }

    /**
     * Create a default property file if it doesn't exist on Central Dogma and
     * return a {@link CentralDogmaPropertySupplier}.
     * @param centralDogmaRepository a {@link CentralDogmaRepository} instance that points to a particular Central Dogma repository.
     * @param filename the name of the file containing properties as top-level fields.
     */
    public static CentralDogmaPropertySupplier register(CentralDogmaRepository centralDogmaRepository, String filename) {
        createPropertyFile(centralDogmaRepository, filename, ProcessorProperties.defaultProperties());
        return new CentralDogmaPropertySupplier(centralDogmaRepository, filename);
    }

    /**
     * Create a default property file if it doesn't exist on Central Dogma and
     * return a {@link CentralDogmaPropertySupplier}.
     * @param centralDogma a {@link CentralDogma} instance to use to access Central Dogma server.
     * @param project the project name where the properties are placed.
     * @param repository the repository name where the properties are placed.
     * @param filename the name of the file containing properties as top-level fields.
     * @param supplier a {@link PropertySupplier} which provides a set of properties with customized initial values.
     */
    public static CentralDogmaPropertySupplier register(CentralDogma centralDogma, String project,
                                                        String repository, String filename,
                                                        PropertySupplier supplier) {
        return register(centralDogma.forRepo(project, repository), filename, supplier);
    }

    /**
     * Create a default property file if it doesn't exist on Central Dogma and
     * return a {@link CentralDogmaPropertySupplier}.
     * @param centralDogmaRepository a {@link CentralDogmaRepository} instance that points to a particular Central Dogma repository.
     * @param filename the name of the file containing properties as top-level fields.
     * @param supplier a {@link PropertySupplier} which provides a set of properties with customized initial values.
     */
    public static CentralDogmaPropertySupplier register(CentralDogmaRepository centralDogmaRepository,
                                                        String filename,
                                                        PropertySupplier supplier) {
        List<Property<?>> properties = ProcessorProperties.defaultProperties().stream().map(defaultProperty -> {
            Optional<? extends Property<?>> prop = supplier.getProperty(defaultProperty.definition());
            if (prop.isPresent()) {
                return prop.get();
            } else {
                return defaultProperty;
            }
        }).collect(Collectors.toList());

        createPropertyFile(centralDogmaRepository, filename, properties);
        return new CentralDogmaPropertySupplier(centralDogmaRepository, filename);
    }

    private static void createPropertyFile(CentralDogmaRepository centralDogmaRepository, String fileName,
                                           List<Property<?>> properties) {
        Revision baseRevision = normalizeRevision(centralDogmaRepository, Revision.HEAD);
        boolean fileExists = fileExists(centralDogmaRepository, fileName, baseRevision);
        long startedTime = System.currentTimeMillis();
        long remainingTime = remainingTime(PROPERTY_CREATION_TIMEOUT_MILLIS, startedTime);

        JsonNode jsonNodeProperties = convertPropertyListToJsonNode(properties);

        while (!fileExists && remainingTime > 0) {
            try {
                centralDogmaRepository
                        .commit(String.format("[CentralDogmaPropertySupplier] Property file created: %s", fileName),
                                Change.ofJsonUpsert(fileName, jsonNodeProperties))
                        .push(baseRevision)
                        .get(remainingTime, TimeUnit.MILLISECONDS);
                logger.info("New property file {} registered on Central Dogma", fileName);
                fileExists = true;
            } catch (ExecutionException e) {
                if (e.getCause() instanceof ChangeConflictException) {
                    logger.warn(
                            "Failed to push to {}. Someone pushed a commit against current revision. Try again",
                            baseRevision);
                    baseRevision = normalizeRevision(centralDogmaRepository, Revision.HEAD);
                    fileExists = fileExists(centralDogmaRepository, fileName, baseRevision);
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

    private static Revision normalizeRevision(CentralDogmaRepository centralDogmaRepository, Revision revision) {
        try {
            return centralDogmaRepository.normalize(revision)
                                         .get(PROPERTY_CREATION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (TimeoutException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    // visible for testing
    static boolean fileExists(CentralDogmaRepository centralDogmaRepository, String filename, Revision revision) {
        try {
            return centralDogmaRepository
                    .file(PathPattern.of(filename))
                    .list(revision)
                    .get(PROPERTY_CREATION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
                    .containsKey(filename);
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
                property -> propertiesObjectNode.set(
                        property.definition().name(),
                        objectMapper.valueToTree(property.value())
                )
        );
        return propertiesObjectNode;
    }
}
