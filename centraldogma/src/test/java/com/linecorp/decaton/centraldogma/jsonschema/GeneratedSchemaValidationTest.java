package com.linecorp.decaton.centraldogma.jsonschema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.SpecVersionDetector;
import com.networknt.schema.ValidationMessage;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GeneratedSchemaValidationTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final List<String> DRAFTS =
            Arrays.asList("draft_7", "draft_2019_09", "draft_2020_12");
    private static final List<String> VARIANTS =
            Arrays.asList("", "-allow-additional-properties");

    private static Path distDir() {
        return Paths.get("src", "jsonschema", "dist");
    }

    private static JsonNode readJson(Path path) throws Exception {
        String s = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
        return MAPPER.readTree(s);
    }

    private static JsonSchema loadSchema(Path path) throws Exception {
        JsonNode schemaNode = readJson(path);
        SpecVersion.VersionFlag version = SpecVersionDetector.detect(schemaNode);
        return JsonSchemaFactory.getInstance(version).getSchema(schemaNode);
    }

    @Test
    void exampleIsValidAcrossAllSchemas() throws Exception {
        Path dir = distDir();
        JsonNode example = readJson(dir.resolve("decaton-processor-properties-central-dogma-example.json"));

        for (String draft : DRAFTS) {
            for (String variant : VARIANTS) {
                String name = String.format(
                        "decaton-processor-properties-central-dogma-schema-%s%s.json",
                        draft, variant
                );
                JsonSchema schema = loadSchema(dir.resolve(name));
                Set<ValidationMessage> errors = schema.validate(example);
                assertTrue(errors.isEmpty(), name + " errors: " + errors);
            }
        }
    }

    @Test
    void additionalPropertiesBehavior() throws Exception {
        Path dir = distDir();
        JsonNode example = readJson(dir.resolve("decaton-processor-properties-central-dogma-example.json"));
        ObjectNode bad = example.deepCopy();
        bad.put("this.should.fail.on.strict", 123);

        for (String draft : DRAFTS) {
            // strict
            {
                String strictName = String.format("decaton-processor-properties-central-dogma-schema-%s.json", draft);
                JsonSchema strict = loadSchema(dir.resolve(strictName));
                Set<ValidationMessage> errors = strict.validate(bad);
                assertFalse(errors.isEmpty(), "Strict should reject extra properties for " + strictName);
            }
            // allow-additional
            {
                String relaxedName = String.format(
                        "decaton-processor-properties-central-dogma-schema-%s-allow-additional-properties.json",
                        draft
                );
                JsonSchema relaxed = loadSchema(dir.resolve(relaxedName));
                Set<ValidationMessage> errors = relaxed.validate(bad);
                assertTrue(
                        errors.isEmpty(),
                        "Relaxed should allow extra properties for " + relaxedName + " but got: " + errors
                );
            }
        }
    }
}
