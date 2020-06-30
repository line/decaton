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

package com.linecorp.decaton.benchmark;

import static com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS;
import static com.fasterxml.jackson.databind.SerializationFeature.WRITE_DURATIONS_AS_TIMESTAMPS;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Duration;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * Format {@link BenchmarkResult} into JSON object.
 */
public class JsonResultFormat implements ResultFormat {
    private static final ObjectWriter writer;
    static {
        // Configure custom serializer to serialize Duration into number in nanos
        ObjectMapper mapper = new ObjectMapper()
                .setVisibility(PropertyAccessor.FIELD, Visibility.ANY)
                .configure(WRITE_DURATIONS_AS_TIMESTAMPS, true)
                .configure(WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS, true);
        JavaTimeModule module = new JavaTimeModule();
        module.addSerializer(Duration.class, new JsonSerializer<Duration>() {
            @Override
            public void serialize(Duration value, JsonGenerator gen, SerializerProvider serializers)
                    throws IOException {
                gen.writeNumber(value.toNanos());
            }
        });
        mapper.registerModule(module);
        writer = mapper.writerWithDefaultPrettyPrinter();
    }

    @Override
    public void print(BenchmarkConfig config, OutputStream out, BenchmarkResult result) throws IOException {
        writer.writeValue(out, result);
    }
}
