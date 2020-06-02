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

import java.io.IOException;
import java.io.OutputStream;

/**
 * An interface to implement formatter for {@link BenchmarkResult}.
 */
public interface ResultFormat {
    /**
     * Format the given {@link BenchmarkResult} and write into {@link OutputStream}.
     * @param config the benchmark config.
     * @param out stream to write formatted output.
     * @param result the {@link BenchmarkResult} to format.
     * @throws IOException when IO error happens.
     */
    void print(BenchmarkConfig config, OutputStream out, BenchmarkResult result) throws IOException;
}
