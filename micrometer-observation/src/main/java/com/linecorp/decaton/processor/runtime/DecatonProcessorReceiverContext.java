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

package com.linecorp.decaton.processor.runtime;

import java.nio.charset.StandardCharsets;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import io.micrometer.observation.transport.Propagator;
import io.micrometer.observation.transport.ReceiverContext;
import lombok.Getter;
import lombok.Setter;

@Getter
public class DecatonProcessorReceiverContext extends ReceiverContext<ConsumerRecord<?, ?>> {
    private final String subscriptionId;

    /**
     *  If the processor has not started processing yet, it returns null.
     */
    @Setter
    private String processorName;

    public DecatonProcessorReceiverContext(ConsumerRecord<?, ?> record, String subscriptionId) {
        super(GETTER);
        this.subscriptionId = subscriptionId;
        setCarrier(record);
    }

    private static final Propagator.Getter<ConsumerRecord<?, ?>> GETTER =
            new Propagator.Getter<ConsumerRecord<?, ?>>() {
                @Override
                public String get(ConsumerRecord<?, ?> carrier, String key) {
                    return lastStringHeader(carrier.headers(), key);
                }

                private String lastStringHeader(Headers headers, String key) {
                    final Header header = headers.lastHeader(key);
                    if (header == null || header.value() == null) {
                        return null;
                    }
                    return new String(header.value(), StandardCharsets.UTF_8);
                }
            };
}
