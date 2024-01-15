/*-
 * #%L
 * Quarkus Kafka Streams Processor
 * %%
 * Copyright (C) 2024 Amadeus s.a.s.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package io.quarkiverse.kafkastreamsprocessor.impl.configuration;

import jakarta.enterprise.context.Dependent;

import io.quarkiverse.kafkastreamsprocessor.api.configuration.Configuration;
import io.quarkiverse.kafkastreamsprocessor.api.configuration.ConfigurationCustomizer;
import io.quarkiverse.kafkastreamsprocessor.impl.TopologyProducer;
import io.quarkus.arc.DefaultBean;

/**
 * Provide a default (empty) implementation of the {@link ConfigurationCustomizer}. It will be injected into the
 * {@link TopologyProducer} unless user provides another implementation.
 */
@Dependent
@DefaultBean
public class DefaultConfigurationCustomizer implements ConfigurationCustomizer {

    /**
     * Default noop version of:
     * <p>
     * {@inheritDoc}
     */
    @Override
    public void fillConfiguration(Configuration config) {
        // Default customizer simply does nothing
    }

}
