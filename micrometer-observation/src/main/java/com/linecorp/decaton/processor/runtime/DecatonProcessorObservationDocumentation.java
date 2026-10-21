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

import io.micrometer.common.docs.KeyName;
import io.micrometer.observation.Observation.Context;
import io.micrometer.observation.ObservationConvention;
import io.micrometer.observation.docs.ObservationDocumentation;

public enum DecatonProcessorObservationDocumentation implements ObservationDocumentation {
    INSTANCE {
        @Override
        public Class<? extends ObservationConvention<? extends Context>> getDefaultConvention() {
            return DefaultDecatonProcessorObservationConvention.class;
        }

        @Override
        public KeyName[] getLowCardinalityKeyNames() {
            return LowCardinalityKeyNames.values();
        }

        @Override
        public KeyName[] getHighCardinalityKeyNames() {
            return HighCardinalityKeyNames.values();
        }
    };

    public enum LowCardinalityKeyNames implements KeyName {
        SUBSCRIPTION_ID {
            @Override
            public String asString() {
                return "subscriptionId";
            }
        },
        PROCESSOR {
            @Override
            public String asString() {
                return "processor";
            }
        },
        TOPIC {
            @Override
            public String asString() {
                return "topic";
            }
        },
        PARTITION {
            @Override
            public String asString() {
                return "partition";
            }
        },
    }

    public enum HighCardinalityKeyNames implements KeyName {
        OFFSET {
            @Override
            public String asString() {
                return "offset";
            }
        },
    }
}
