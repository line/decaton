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

package com.linecorp.decaton;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.asciidoctor.Asciidoctor;
import org.asciidoctor.ast.DocumentHeader;
import org.asciidoctor.jruby.internal.IOUtils;

import lombok.Value;
import lombok.experimental.Accessors;

public final class DocumentChecker {
    @Value
    @Accessors(fluent = true)
    private static class Version {
        private static final Pattern RE = Pattern.compile("([0-9]+)\\.([0-9]+)\\.([0-9]+)(-SNAPSHOT)?");
        int major;
        int minor;
        int patch;
        boolean snapshot;

        static Version parse(String string) {
            Matcher matcher = RE.matcher(string);
            if (!matcher.matches()) {
                throw new IllegalArgumentException("invalid format");
            }
            return new Version(Integer.parseInt(matcher.group(1)),
                               Integer.parseInt(matcher.group(2)),
                               Integer.parseInt(matcher.group(3)),
                               matcher.group(4) != null);
        }

        boolean hasHigherOrEqualMajorVersion(Version other) {
            return major >= other.major;
        }

        @Override
        public String toString() {
            return String.valueOf(major) + '.' + minor + '.' + patch + (snapshot ? "-SNAPSHOT" : "");
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.err.println("Usage: com.linecorp.decaton.DocumentChecker VERSION DOC_DIR");
            System.exit(1);
        }

        int index = 0;
        Version currentVersion = Version.parse(args[index++]);
        String docDir = args[index++];

        boolean anyError = false;
        try (DirectoryStream<Path> files = Files.newDirectoryStream(Paths.get(docDir), "*.adoc")) {
            DocumentChecker checker = new DocumentChecker();

            for (Path file : files) {
                List<String> errors = checker.validate(file, currentVersion);
                if (!errors.isEmpty()) {
                    System.err.println("# Errors in " + file);
                    for (String error : errors) {
                        System.err.println("* " + error);
                    }
                    anyError = true;
                }
            }
        }
        if (anyError) {
            System.exit(1);
        }
    }

    private final Asciidoctor asciidoctor;

    public DocumentChecker() {
        asciidoctor = Asciidoctor.Factory.create();
    }

    private List<String> validate(Path file, Version version) throws IOException {
        DocumentHeader header = asciidoctor.readDocumentHeader(file.toFile());
        Map<String, Object> attrs = header.getAttributes();

        List<String> errors = new ArrayList<>();

        String baseVer = (String) attrs.get("base_version");
        if (baseVer == null) {
            errors.add("missing attribute `base_version`");
        }
        String modulesList = (String) attrs.get("modules");
        if (modulesList == null) {
            errors.add("missing attribute `modules`");
        }
        if (!errors.isEmpty()) {
            return errors;
        }

        Version baseVersion = Version.parse(baseVer);
        if (!version.snapshot() && baseVersion.snapshot()) {
            errors.add(
                    String.format("base_version of SNAPSHOT (%s) isn't allowed when making release build (%s)",
                                  baseVersion, version));
            return errors;
        }
        if (baseVersion.hasHigherOrEqualMajorVersion(version)) {
            return errors;
        }

        String[] modules = modulesList.split(",");
        Set<String> updatedModules = findUpdatedModules(baseVersion, modules);
        for (String updatedModule : updatedModules) {
            errors.add(String.format("module %s updated after base_version=%s", updatedModule, baseVersion));
        }

        return errors;
    }

    private static Set<String> findUpdatedModules(Version baseVersion, String[] modules) throws IOException {
        String[] cmd = { "git", "diff", "--name-only", "tags/v" + baseVersion, "HEAD" };
        Process proc = Runtime.getRuntime().exec(cmd);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(proc.getInputStream()))) {
            Set<String> updatedMods = new HashSet<>();
            reader.lines().forEach(line -> {
                for (String module : modules) {
                    if (line.startsWith(module + '/')) {
                        updatedMods.add(module);
                    }
                }
            });

            return updatedMods;
        } finally {
            try {
                proc.waitFor();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.err.println("Interrupted awaiting git commands exit");
            }

            if (proc.exitValue() != 0) {
                System.err.printf("git command %s failed with output: %s\n",
                                  Arrays.toString(cmd), IOUtils.readFull(proc.getErrorStream()));
                throw new RuntimeException("git command failed");
            }
        }
    }
}
