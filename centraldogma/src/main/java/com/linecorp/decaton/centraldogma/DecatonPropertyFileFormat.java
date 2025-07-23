/*
 * Copyright 2025 LINE Corporation
 *
 * Licensed under the Apache License, Version 2.0 …
 */

package com.linecorp.decaton.centraldogma;

import java.io.IOException;
import java.util.Locale;

import com.fasterxml.jackson.databind.JsonNode;
import com.linecorp.centraldogma.client.CentralDogmaRepository;
import com.linecorp.centraldogma.client.Watcher;
import com.linecorp.centraldogma.common.Change;

/**
 * Encapsulates Central Dogma–specific concerns for reading and writing
 * configuration files in various text formats (JSON, YAML, ...).
 * <p>
 * Implementations convert between raw file contents managed by Central Dogma
 * and {@link JsonNode} values consumed by {@link CentralDogmaPropertySupplier}.
 */
public interface DecatonPropertyFileFormat {
    /**
     * Create and start a Watcher that emits {@link JsonNode} for each file update.
     */
    Watcher<JsonNode> createWatcher(CentralDogmaRepository repo, String fileName);

    /**
     * Serialize the given node and wrap it as Central Dogma {@link Change} for initial file creation.
     */
    Change<?> createUpsertChange(String fileName, JsonNode initialNode) throws IOException;

    static DecatonPropertyFileFormat of(String fileName) {
        String lower = fileName.toLowerCase(Locale.ROOT);
        return (lower.endsWith(".yml") || lower.endsWith(".yaml"))
                ? new YamlFormat()
                : new JsonFormat();
    }
}
