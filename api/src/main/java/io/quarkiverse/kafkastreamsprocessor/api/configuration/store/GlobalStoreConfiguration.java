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
package io.quarkiverse.kafkastreamsprocessor.api.configuration.store;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

/**
 * Configuration class for defining a global state store in Kafka Streams.
 *
 * @param <K> The type of keys stored in the state store.
 * @param <V> The type of values stored in the state store.
 */
@ToString
@Getter
@AllArgsConstructor
public final class GlobalStoreConfiguration<K, V> {

    /**
     * The builder for creating the state store.
     * This defines the properties and behavior of the store.
     */
    private final StoreBuilder<? extends KeyValueStore<K, V>> storeBuilder;

    /**
     * The deserializer for the keys in the state store.
     * Used to deserialize keys from the Kafka topic into the store.
     */
    private final Deserializer<K> keyDeserializer;

    /**
     * The deserializer for the values in the state store.
     * Used to deserialize values from the Kafka topic into the store.
     */
    private final Deserializer<V> valueDeserializer;

    /**
     * The supplier for the processor associated with the global state store.
     * This processor is responsible for processing records for the store.
     * This processor supplier must return new instances every time it is called
     * supplier.get() is called twice by Kafka Streams to verify this constraint
     */
    private final GlobalStoreProcessorSupplier<K, V> globalStoreProcessorSupplier;
}
