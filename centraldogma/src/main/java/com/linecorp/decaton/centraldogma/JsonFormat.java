/*
 * Copyright 2025 LINE Corporation
 */

package com.linecorp.decaton.centraldogma;

import com.fasterxml.jackson.databind.JsonNode;
import com.linecorp.centraldogma.client.CentralDogmaRepository;
import com.linecorp.centraldogma.client.Watcher;
import com.linecorp.centraldogma.common.Change;
import com.linecorp.centraldogma.common.Query;

public class JsonFormat implements DecatonPropertyFileFormat {
    @Override
    public Watcher<JsonNode> createWatcher(CentralDogmaRepository repo, String fileName) {
        return repo.watcher(Query.ofJsonPath(fileName)).start();
    }

    @Override
    public Change<?> createUpsertChange(String fileName, JsonNode initialNode) {
        return Change.ofJsonUpsert(fileName, initialNode);
    }
}
