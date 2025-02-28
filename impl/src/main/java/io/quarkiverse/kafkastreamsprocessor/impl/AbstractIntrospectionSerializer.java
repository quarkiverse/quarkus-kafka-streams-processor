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

import org.apache.kafka.common.serialization.Serializer;

import com.google.protobuf.MessageLite;

/**
 * Bridge serializer based on introspection that allows another serialization policy if the data is not protobuf..
 */
public abstract class AbstractIntrospectionSerializer implements Serializer<Object> {
    /**
     * {@inheritDoc}
     */
    @Override
    // Returning null is a valid behaviour here
    @SuppressWarnings("java:S1168")
    public byte[] serialize(String topic, Object data) {
        // null needs to treated specially since the client most likely just wants to send
        // an individual null value. null in Kafka has a special meaning for deletion in a
        // topic with the compact retention policy.
        if (data == null) {
            return null;
        }
        if (data instanceof MessageLite) {
            return ((MessageLite) data).toByteArray();
        } else {
            return serializeNonProtobuf(data);
        }
    }

    abstract byte[] serializeNonProtobuf(Object data);
}
