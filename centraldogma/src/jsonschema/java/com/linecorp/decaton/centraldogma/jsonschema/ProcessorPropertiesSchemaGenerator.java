/*
 * Copyright 2025 LY Corporation
 *
 * LY Corporation licenses this file to you under the Apache License,
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

package com.linecorp.decaton.centraldogma.jsonschema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.victools.jsonschema.generator.Option;
import com.github.victools.jsonschema.generator.OptionPreset;
import com.github.victools.jsonschema.generator.SchemaGenerator;
import com.github.victools.jsonschema.generator.SchemaGeneratorConfigBuilder;
import com.github.victools.jsonschema.generator.SchemaVersion;
import com.github.therapi.runtimejavadoc.FieldJavadoc;
import com.github.therapi.runtimejavadoc.RuntimeJavadoc;
import com.linecorp.decaton.processor.runtime.ProcessorProperties;
import com.linecorp.decaton.processor.runtime.PropertyDefinition;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Generates JSON schema files for Decaton ProcessorProperties.
 * And more, it generates example JSON file for Central Dogma.
 * The generated schemas are compatible with some JSON Schema.
 */
@Slf4j
public final class ProcessorPropertiesSchemaGenerator {

    private static final List<SchemaVersion> TARGET_VERSIONS = List.of(
            SchemaVersion.DRAFT_7,
            SchemaVersion.DRAFT_2019_09,
            SchemaVersion.DRAFT_2020_12
    );
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static Map<PropertyDefinition<?>, Type> buildTypeTable() {
        Map<PropertyDefinition<?>, Type> table = new HashMap<>();
        for (Field field : ProcessorProperties.class.getDeclaredFields()) {
            if (!PropertyDefinition.class.isAssignableFrom(field.getType())) continue;

            try {
                PropertyDefinition<?> def = (PropertyDefinition<?>) field.get(null);
                Type valueType = switch (field.getGenericType()) {
                    case ParameterizedType pt -> pt.getActualTypeArguments()[0];  // List<String> etc
                    default -> def.runtimeType(); // Long.class etc
                };
                table.put(def, valueType);
            } catch (IllegalAccessException e) {
                throw new RuntimeException("Unable to access field " + field.getName(), e);
            }
        }
        return table;
    }

    private static Map<PropertyDefinition<?>, String> buildDocTable() {
        Map<PropertyDefinition<?>, String> docs = new HashMap<>();
        for (Field field : ProcessorProperties.class.getDeclaredFields()) {
            if (!PropertyDefinition.class.isAssignableFrom(field.getType())) continue;

            try {
                PropertyDefinition<?> def = (PropertyDefinition<?>) field.get(null);
                FieldJavadoc fj = RuntimeJavadoc.getJavadoc(field);
                if (!fj.isEmpty()) {
                    String txt = fj.getComment().toString().trim();
                    docs.put(def, txt);
                }
            } catch (IllegalAccessException e) {
                throw new RuntimeException("Unable to access field " + field.getName(), e);
            }
        }
        return docs;
    }

    /**
     * Main method to generate JSON schema files for Decaton ProcessorProperties for Central Dogma.
     *
     * @param args args[0] should be the output directory where the schema files will be written.
     *             args[1] should be the Decaton version to be used in the generated schema.
     * @throws IOException if an I/O error occurs while writing the schema files.
     */
    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.println("usage: <outDir> <decatonVersion>");
            System.exit(1);
        }
        Path outDir = Paths.get(args[0]);
        Files.createDirectories(outDir);
        String decatonVersion = args[1];

        for (SchemaVersion draft : TARGET_VERSIONS) {
            generateForDraft(outDir, draft, false, decatonVersion);
            generateForDraft(outDir, draft, true, decatonVersion);
        }
        generateCentralDogmaJsonExample(outDir, decatonVersion);
    }

    private static void generateForDraft(
            Path dir,
            SchemaVersion draft,
            boolean allowAdditional,
            String decatonVersion
    ) throws IOException {
        String fileName = String.format(
                "decaton-processor-properties-central-dogma-schema-%s%s.json",
                draft.name().toLowerCase(),
                allowAdditional ? "-allow-additional-properties" : ""
        );
        Path file = dir.resolve(fileName);

        JsonNode schema = buildSchema(draft, allowAdditional, decatonVersion);
        Files.writeString(file, MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(schema) + "\n");
        log.info("wrote {}", file);
    }

    private static JsonNode buildSchema(
            SchemaVersion draft,
            boolean allowAdditional,
            String decatonVersion
    ) {
        Map<PropertyDefinition<?>, Type> typeTable = buildTypeTable();
        Map<PropertyDefinition<?>, String> docTable = buildDocTable();

        SchemaGenerator generator = new SchemaGenerator(
                new SchemaGeneratorConfigBuilder(MAPPER, draft, OptionPreset.PLAIN_JSON)
                        .without(Option.SCHEMA_VERSION_INDICATOR)
                        .build());

        var root = MAPPER.createObjectNode();
        root.put("$schema", draft.getIdentifier());
        root.put(
                "title",
                "Decaton ProcessorProperties for CentralDogma (decaton=%s, jsonschema=%s, strict=%s)"
                        .formatted(decatonVersion, draft.name().toLowerCase(), !allowAdditional)
        );
        root.put("type", "object");
        root.put("additionalProperties", allowAdditional);
        var required = root.putArray("required");

        var props = root.putObject("properties");
        // Allow instance to write $schema property
        props.putObject("$schema").put("type", "string");

        for (PropertyDefinition<?> def : ProcessorProperties.PROPERTY_DEFINITIONS) {
            Type valueType = typeTable.get(def);
            var node = generator.generateSchema(valueType);
            if (def.defaultValue() != null) {
                node.set("default", MAPPER.valueToTree(def.defaultValue()));
            } else {
                required.add(def.name());
            }

            String doc = docTable.getOrDefault(def, "").replaceAll("\\s+", " ").trim();
            node.put("description", doc);

            props.set(def.name(), node);
        }
        return root;
    }

    private static void generateCentralDogmaJsonExample(
            Path dir,
            String decatonVersion
    ) throws IOException {
        var root = MAPPER.createObjectNode();
        root.put("$schema",
                "https://raw.githubusercontent.com/line/decaton/v%s/centraldogma/src/jsonschema/dist/decaton-processor-properties-central-dogma-schema-draft_7.json"
                        .formatted(decatonVersion));

        for (PropertyDefinition<?> def : ProcessorProperties.PROPERTY_DEFINITIONS) {
            if (def.defaultValue() != null) {
                root.set(def.name(), MAPPER.valueToTree(def.defaultValue()));
            }
        }
        Path file = dir.resolve("decaton-processor-properties-central-dogma-example.json");
        Files.writeString(file, MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(root) + "\n");
        log.info("wrote {}", file);
    }

    private ProcessorPropertiesSchemaGenerator() {
    }
}
