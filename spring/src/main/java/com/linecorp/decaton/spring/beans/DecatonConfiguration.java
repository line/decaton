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

package com.linecorp.decaton.spring.beans;

import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.linecorp.decaton.spring.DecatonSpring;
import com.linecorp.decaton.spring.PartitionScope;
import com.linecorp.decaton.spring.SubpartitionScope;

@SuppressWarnings("NonFinalUtilityClass")
@Configuration
public class DecatonConfiguration {
    public static class DecatonBeanFactoryPostProcessor implements BeanFactoryPostProcessor {
        @Override
        public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) {
            beanFactory.registerScope(DecatonSpring.PARTITION_SCOPE, new PartitionScope());
            beanFactory.registerScope(DecatonSpring.SUBPARTITION_SCOPE, new SubpartitionScope());
        }
    }

    @Bean
    public static BeanFactoryPostProcessor decatonBeanFactoryPostProcessor() {
        return new DecatonBeanFactoryPostProcessor();
    }
}
