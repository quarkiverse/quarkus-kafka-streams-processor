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
import org.apache.kafka.common.serialization.Deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.RequiredArgsConstructor;

/**
 * Message deserializer for JSON content. Based on the well-known Jackson Java library.
 *
 * @param <T>
 *        the message type to deserialize
 */
@RequiredArgsConstructor
public class JacksonDeserializer<T> implements Deserializer<T> {
    /**
     * The type of the messages to deserialize.
     */
    private final Class<T> targetClass;

    /**
     * The actual Jackson object matter. IT can be set manually and use the default mapper exposed by Quarkus.
     */
    private final ObjectMapper objectMapper;

    /**
     * Constructor that instantiates its own instance of {@link ObjectMapper}
     *
     * @param targetClass
     *        Deserialized data type
     */
    public JacksonDeserializer(Class<T> targetClass) {
        this(targetClass, new ObjectMapper());
    }

    /**
     * {@inheritDoc}
     *
     * @throws SerializationException
     *         if any exception was encountered during deserialization
     */
    @Override
    public T deserialize(String s, byte[] bytes) {
        return deserialize(s, null, bytes);
    }

    /**
     * {@inheritDoc}
     *
     * @throws SerializationException
     *         if any exception was encountered during deserialization
     */
    @Override
    public T deserialize(String topic, Headers headers, byte[] data) {
        if (data == null) {
            return null;
        }
        T pojo;
        try {
            pojo = objectMapper.readValue(data, targetClass);
        } catch (Exception e) {
            throw new SerializationException("Error deserializing JSON message into Pojo object", e);
        }
        return pojo;
    }

}
