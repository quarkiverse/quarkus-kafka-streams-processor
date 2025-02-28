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
package io.quarkiverse.kafkastreamsprocessor.impl;

import jakarta.inject.Inject;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Bridge serializer based on introspection that allows a JSON first input message policy.
 */
public class IntrospectionSerializer extends AbstractIntrospectionSerializer {
    private final ObjectMapper objectMapper;

    /**
     * Injection constructor
     *
     * @param objectMapper
     *        the {@link ObjectMapper} instance created by the <code>quarkus-jackson</code> extension
     */
    @Inject
    public IntrospectionSerializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    byte[] serializeNonProtobuf(Object data) {
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Message cannot be processed, " +
                    "one of the following payloads is expected: valid JSON Pojo or Protobuf", e);
        }
    }
}
