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

import java.util.Collections;
import java.util.List;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import io.quarkiverse.kafkastreamsprocessor.api.configuration.Configuration;
import io.quarkiverse.kafkastreamsprocessor.api.configuration.store.StoreConfiguration;
import lombok.Data;

/**
 * Implementation for {@link Configuration}.
 * <p>
 * Used as a container for configuration customization.
 * </p>
 */
@Data
public class TopologyConfigurationImpl implements Configuration {

    private final Class<?> processorPayloadType;
    private Serializer<?> sinkValueSerializer;
    private Serde<?> sourceValueSerde;
    private List<StoreConfiguration> storeConfigurations = Collections.emptyList();

    /**
     * Configuration constructor
     *
     * @param processorPayloadType
     *        the input payload type
     */
    public TopologyConfigurationImpl(Class<?> processorPayloadType) {
        this.processorPayloadType = processorPayloadType;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setStoreConfigurations(List<StoreConfiguration> storeConfigurations) {
        this.storeConfigurations = Collections.unmodifiableList(storeConfigurations);
    }
}
