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
package io.quarkiverse.kafkastreamsprocessor.api.serdes;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.RequiredArgsConstructor;

/**
 * Message serializer for JSON content. Based on the well-known Jackson Java library.
 *
 * @param <T>
 *        the message type to serialize
 */
@RequiredArgsConstructor
public class JacksonSerializer<T> implements Serializer<T> {
    /**
     * The actual Jackson object matter. IT can be set manually and use the default mapper exposed by Quarkus.
     */
    private final ObjectMapper objectMapper;

    /**
     * Constructor that instantiates its own instance of {@link ObjectMapper}
     */
    public JacksonSerializer() {
        this(new ObjectMapper());
    }

    /**
     * {@inheritDoc}
     *
     * @throws SerializationException
     *         if any exception was encountered during serialization
     */
    @Override
    public byte[] serialize(String topic, T myPojo) {
        return serialize(topic, null, myPojo);
    }

    /**
     * {@inheritDoc}
     *
     * @throws SerializationException
     *         if any exception was encountered during serialization
     */
    @Override
    public byte[] serialize(String topic, Headers headers, T myPojo) {
        if (myPojo == null) {
            return null;
        }
        try {
            return objectMapper.writeValueAsBytes(myPojo);
        } catch (JsonProcessingException e) {
            throw new SerializationException("Error serializing Pojo object into bytes", e);
        }
    }
}
