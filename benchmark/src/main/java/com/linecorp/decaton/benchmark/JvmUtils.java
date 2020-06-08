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

import java.lang.management.ManagementFactory;

public final class JvmUtils {
    private JvmUtils() {}

    /**
     * Get current process's PID.
     * @return pid of the running JVM.
     */
    public static long currentPid() {
        String[] names = ManagementFactory.getRuntimeMXBean().getName().split("@", 2);
        return Long.parseLong(names[0]);
    }

    /**
     * Detect operating system is Linux or else.
     * @return true if the operating system is Linux, false otherwise.
     */
    public static boolean isOsLinux() {
        String osName = System.getProperty("os.name");
        if (osName == null) {
            throw new RuntimeException("system property os.name isn't available");
        }
        return osName.startsWith("Linux");
    }
}
