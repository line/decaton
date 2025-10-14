/*
 * Copyright 2025 LINE Corporation
 *
 * Licensed under the Apache License, Version 2.0 …
 */

package com.linecorp.decaton.centraldogma.internal;

import java.io.IOException;
import java.io.UncheckedIOException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.linecorp.centraldogma.client.CentralDogmaRepository;
import com.linecorp.centraldogma.client.Watcher;
import com.linecorp.centraldogma.common.Change;
import com.linecorp.centraldogma.common.Query;

import static com.fasterxml.jackson.dataformat.yaml.YAMLGenerator.Feature.WRITE_DOC_START_MARKER;

public class YamlFormat implements DecatonPropertyFileFormat {
    private static final ObjectMapper YAML_MAPPER = new ObjectMapper(
            new YAMLFactory()
                    .disable(WRITE_DOC_START_MARKER)
    );

    @Override
    public Watcher<JsonNode> createWatcher(CentralDogmaRepository repo, String fileName) {
        return repo.watcher(Query.ofText(fileName))
                .map(text -> {
                    try {
                        return YAML_MAPPER.readTree(text);
                    } catch (IOException e) {
                        throw new UncheckedIOException("Failed to parse YAML from " + fileName, e);
                    }
                })
                .start();
    }

    @Override
    public Change<?> createUpsertChange(String fileName, JsonNode initialNode) throws IOException {
        String yaml = YAML_MAPPER.writeValueAsString(initialNode);
        return Change.ofTextUpsert(fileName, yaml);
    }
}
