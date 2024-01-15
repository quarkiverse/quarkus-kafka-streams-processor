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
package io.quarkiverse.kafkastreamsprocessor.sample.jsonpojo;

import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.quarkiverse.kafkastreamsprocessor.api.configuration.Configuration;
import io.quarkiverse.kafkastreamsprocessor.api.configuration.ConfigurationCustomizer;
import io.quarkiverse.kafkastreamsprocessor.api.serdes.JacksonSerde;
import io.quarkiverse.kafkastreamsprocessor.api.serdes.JacksonSerializer;

@Dependent
public class SampleConfigurationCustomizer implements ConfigurationCustomizer {

    private final ObjectMapper objectMapper;

    @Inject
    public SampleConfigurationCustomizer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public void fillConfiguration(Configuration configuration) {
        configuration.setSinkValueSerializer(new JacksonSerializer<>(objectMapper));
        configuration.setSourceValueSerde(new JacksonSerde<>(configuration.getProcessorPayloadType(), objectMapper));
    }
}
