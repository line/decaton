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

import java.util.Optional;

import com.linecorp.decaton.processor.runtime.DecatonProcessorObservationDocumentation.HighCardinalityKeyNames;
import com.linecorp.decaton.processor.runtime.DecatonProcessorObservationDocumentation.LowCardinalityKeyNames;

import io.micrometer.common.KeyValues;

class DefaultDecatonProcessorObservationConvention implements DecatonProcessorObservationConvention {
    @Override
    public KeyValues getLowCardinalityKeyValues(DecatonProcessorReceiverContext context) {
        return KeyValues.of(
                LowCardinalityKeyNames.SUBSCRIPTION_ID.withValue(context.getSubscriptionId()),
                LowCardinalityKeyNames.PROCESSOR.withValue(
                        Optional.ofNullable(context.getProcessorName()).orElse("-")),
                LowCardinalityKeyNames.TOPIC.withValue(context.getCarrier().topic()),
                LowCardinalityKeyNames.PARTITION.withValue(String.valueOf(context.getCarrier().partition()))
        );
    }

    @Override
    public KeyValues getHighCardinalityKeyValues(DecatonProcessorReceiverContext context) {
        return KeyValues.of(
                HighCardinalityKeyNames.OFFSET.withValue(String.valueOf(context.getCarrier().offset()))
        );
    }

    @Override
    public String getContextualName(DecatonProcessorReceiverContext context) {
        return String.format(
                "decaton processor (%s/%s)",
                context.getSubscriptionId(),
                context.getProcessorName());
    }
}
