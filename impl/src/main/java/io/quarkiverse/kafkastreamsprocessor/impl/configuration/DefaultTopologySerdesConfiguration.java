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

import static io.quarkiverse.kafkastreamsprocessor.impl.configuration.TypeUtils.createParserFromType;

import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.daniel.shuy.kafka.protobuf.serde.KafkaProtobufSerde;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;

import io.quarkiverse.kafkastreamsprocessor.api.configuration.Configuration;
import io.quarkiverse.kafkastreamsprocessor.api.configuration.ConfigurationCustomizer;
import io.quarkiverse.kafkastreamsprocessor.api.serdes.JacksonSerde;
import io.quarkiverse.kafkastreamsprocessor.impl.IntrospectionSerializer;
import io.quarkiverse.kafkastreamsprocessor.impl.StringIntrospectionSerializer;

/**
 * Default serde configuration for the {@link Topology}.
 * <p>
 * The default format is JSON, based on the Jackson {@link ObjectMapper} configured by the <code>quarkus-jackson</code>
 * extension, on the classpath through the <code>quarkus-kafka-streams</code> module.
 * </p>
 * <p>
 * To use custom serdes use {@link ConfigurationCustomizer}.
 * </p>
 */
@Dependent
public class DefaultTopologySerdesConfiguration {

    private final ObjectMapper objectMapper;

    /**
     * Injection constructor
     *
     * @param objectMapper
     *        the object mapper producer by <code>quarkus-jackson</code> extension
     */
    @Inject
    public DefaultTopologySerdesConfiguration(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /**
     * Apply the serdes configuration
     *
     * @param topologyConfiguration
     *        the configuration object after the customization
     */
    public void apply(Configuration topologyConfiguration) {
        TopologyConfigurationImpl configuration = (TopologyConfigurationImpl) topologyConfiguration;
        configureSinkValueSerializer(configuration);
        configureSourceValueSerde(configuration);
    }

    private void configureSourceValueSerde(TopologyConfigurationImpl configuration) {
        if (configuration.getSourceKeySerde() == null) {
            if (MessageLite.class.isAssignableFrom(configuration.getProcessorKeyType())) {
                Parser<? extends MessageLite> parser = createParserFromType(configuration.getProcessorKeyType());
                configuration.setSourceKeySerde(new KafkaProtobufSerde<>(parser));
            } else {
                configuration.setSourceKeySerde(Serdes.String());
            }
        }
        if (configuration.getSourceValueSerde() == null) {
            if (MessageLite.class.isAssignableFrom(configuration.getProcessorPayloadType())) {
                Parser<? extends MessageLite> parser = createParserFromType(configuration.getProcessorPayloadType());
                configuration.setSourceValueSerde(new KafkaProtobufSerde<>(parser));
            } else {
                configuration.setSourceValueSerde(new JacksonSerde(configuration.getProcessorPayloadType(), objectMapper));
            }
        }
    }

    private void configureSinkValueSerializer(TopologyConfigurationImpl configuration) {
        if (configuration.getSinkKeySerializer() == null) {
            configuration.setSinkKeySerializer(new StringIntrospectionSerializer());
        }
        if (configuration.getSinkValueSerializer() == null) {
            configuration.setSinkValueSerializer(new IntrospectionSerializer(objectMapper));
        }
    }
}
