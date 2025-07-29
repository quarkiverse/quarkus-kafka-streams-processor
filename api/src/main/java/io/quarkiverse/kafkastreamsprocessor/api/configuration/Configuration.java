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
package io.quarkiverse.kafkastreamsprocessor.api.configuration;

import java.util.List;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import io.quarkiverse.kafkastreamsprocessor.api.Processor;
import io.quarkiverse.kafkastreamsprocessor.api.configuration.store.GlobalStoreConfiguration;
import io.quarkiverse.kafkastreamsprocessor.api.configuration.store.StoreConfiguration;

/**
 * This is an interface for accessing configuration elements needed by the SDK to build the KStreams Topology. In
 * particular, it contains setters for the serializer and deserializer. If you want to customize the configuration, you
 * need to provide a {@link ConfigurationCustomizer} implementation to be injected into the SDK at run-time for it to
 * take it into account.
 */
public interface Configuration {

    /**
     * Class representing the type of keys handled by the {@link Processor}.
     * <p>
     * Default is {@link String}.
     * </p>
     */
    Class<?> getProcessorKeyType();

    /**
     * Class representing the type of values handled by the {@link Processor}. This value
     * is initialized internally by the TopologyProducer. It is read-only because you shouldn't modify it, although you
     * can read it.
     * <p>
     * It is primarily used to instantiate the default value deserializer using generics.
     */
    Class<?> getProcessorPayloadType();

    /**
     * Serde for the keys fed to the {@link Processor}. Use the setter if you want to
     * override it.
     */
    void setSourceKeySerde(Serde<?> serde);

    /**
     * @return The serde for keys fed to the {@link Processor}
     */
    Serde<?> getSourceKeySerde();

    /**
     * Serde for the value fed to the {@link Processor}. Use the setter if you want to
     * override it.
     */
    void setSourceValueSerde(Serde<?> serde);

    /**
     * @return The serde for values fed to the {@link Processor}
     */
    Serde<?> getSourceValueSerde();

    /**
     * Serializer for values forwarded by the {@link Processor}. Use the setter if you want
     * to override it.
     */
    void setSinkKeySerializer(Serializer<?> serializer);

    /**
     * @return The serializer for values forwarded by the {@link Processor}
     */
    Serializer<?> getSinkKeySerializer();

    /**
     * Serializer for values forwarded by the {@link Processor}. Use the setter if you want
     * to override it.
     */
    void setSinkValueSerializer(Serializer<?> serializer);

    /**
     * @return The serializer for values forwarded by the {@link Processor}
     */
    Serializer<?> getSinkValueSerializer();

    /**
     * Store configuration to be used by the {@link Processor}. Use the setter if you want
     * to override it.
     */
    void setStoreConfigurations(List<StoreConfiguration> storeConfigurations);

    /**
     * @return The state store configuration to be used by the {@link Processor}
     */
    List<StoreConfiguration> getStoreConfigurations();

    /**
     * Store configuration to be used by the {@link Processor}. Use setter if you want to override it.
     */
    void setGlobalStoreConfigurations(List<GlobalStoreConfiguration> storeConfiguration);

    /**
     * @return the global state store configuration to be used by the {@link Processor}
     */
    List<GlobalStoreConfiguration> getGlobalStoreConfigurations();
}
