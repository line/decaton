/*
 * Copyright 2026 LY Corporation
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

package com.linecorp.decaton.processor.runtime.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Test;

public class ConsumerSupplierTest {
    @Test
    public void testAutoOffsetResetDefaultsToEarliest() {
        Properties props = new ConsumerSupplier(new Properties()).mergedProps();

        assertEquals("earliest", props.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
    }

    @Test
    public void testAutoOffsetResetCanBeOverridden() {
        Properties config = new Properties();
        config.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        Properties props = new ConsumerSupplier(config).mergedProps();

        assertEquals("latest", props.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
    }
}
