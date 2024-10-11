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
package io.quarkiverse.kafkastreamsprocessor.sample.customserde;

import java.nio.charset.StandardCharsets;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.apache.kafka.common.serialization.Deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

@ApplicationScoped
@Slf4j
public class CustomTypeDeserializer implements Deserializer<CustomType> {
    private final ObjectMapper objectMapper;

    @Inject
    public CustomTypeDeserializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public CustomType deserialize(String topic, byte[] data) {
        try {
            CustomType readValue = objectMapper.readValue(data, CustomType.class);
            return new CustomType(readValue.getValue() - CustomTypeSerde.SHIFT);
        } catch (Exception e) {
            log.error("Could not deserialize: {}", new String(data, StandardCharsets.UTF_8));
            throw new RuntimeException("Error deserializing CustomType", e);
        }
    }
}
