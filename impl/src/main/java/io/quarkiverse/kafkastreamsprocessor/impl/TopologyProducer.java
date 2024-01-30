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

import java.util.Map;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.enterprise.inject.spi.BeanManager;
import jakarta.inject.Inject;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;

import io.quarkiverse.kafkastreamsprocessor.api.configuration.ConfigurationCustomizer;
import io.quarkiverse.kafkastreamsprocessor.api.decorator.producer.ProducerOnSendInterceptor;
import io.quarkiverse.kafkastreamsprocessor.impl.configuration.DefaultConfigurationCustomizer;
import io.quarkiverse.kafkastreamsprocessor.impl.configuration.DefaultTopologySerdesConfiguration;
import io.quarkiverse.kafkastreamsprocessor.impl.configuration.TopologyConfigurationImpl;
import io.quarkiverse.kafkastreamsprocessor.impl.configuration.TypeUtils;
import io.quarkiverse.kafkastreamsprocessor.spi.properties.KStreamsProcessorConfig;

/**
 * Processor must be annotated with {@link io.quarkiverse.kafkastreamsprocessor.api.Processor}
 */
@ApplicationScoped
public class TopologyProducer {
    /**
     * Default name attached to the unique {@link Processor} this topology produces
     */
    public static final String PROCESSOR_NAME = "Processor";

    /**
     * Default sink name of the dead-letter queue
     */
    public static final String DLQ_SINK_NAME = "DLQ";

    /**
     * Class containing the configuration related to kafka streams processor
     */
    private final KStreamsProcessorConfig kStreamsProcessorConfig;

    /**
     * The configuration customizer if any defined by the microservice.
     * <p>
     * If not defined the {@link DefaultConfigurationCustomizer} is injected.
     * </p>
     */
    private final ConfigurationCustomizer configCustomizer;

    /**
     * The source configuration bean which produces the mapping between source and their respective topics
     */
    private final SourceToTopicsMappingBuilder sourceToTopicsMappingBuilder;

    /**
     * The sink configuration bean which resolves the mapping between sink and their respective Kafka topic
     */
    private final SinkToTopicMappingBuilder sinkToTopicMappingBuilder;

    /**
     * Producer interceptor list that allow to intercept the production of messages to Kafka
     */
    private final Instance<ProducerOnSendInterceptor> interceptors;

    /**
     * Injection constructor
     *
     * @param kStreamsProcessorConfig
     *        Class containing the configuration related to kafka streams processor
     * @param configCustomizer
     *        The configuration customizer if any defined by the microservice.
     * @param sourceToTopicsMappingBuilder
     *        The source configuration bean which produces the mapping between source and their respective topics
     * @param sinkToTopicMappingBuilder
     *        The sink configuration bean which resolves the mapping between sink and their respective Kafka topic
     * @param interceptors
     *        Producer interceptor list that allow to intercept the production of messages to Kafka
     */
    @Inject
    public TopologyProducer(KStreamsProcessorConfig kStreamsProcessorConfig,
            ConfigurationCustomizer configCustomizer, SourceToTopicsMappingBuilder sourceToTopicsMappingBuilder,
            SinkToTopicMappingBuilder sinkToTopicMappingBuilder, Instance<ProducerOnSendInterceptor> interceptors) {
        this.kStreamsProcessorConfig = kStreamsProcessorConfig;
        this.configCustomizer = configCustomizer;
        this.sourceToTopicsMappingBuilder = sourceToTopicsMappingBuilder;
        this.sinkToTopicMappingBuilder = sinkToTopicMappingBuilder;
        this.interceptors = interceptors;
    }

    /**
     * Injection of the {@link KafkaClientSupplier} that decorated client to intercept message production with the
     * {@link ProducerOnSendInterceptor} defined.
     */
    @Produces
    public KafkaClientSupplier kafkaClientSupplier() {
        return new KafkaClientSupplierDecorator(interceptors);
    }

    /**
     * Produces the final {@link TopologyConfigurationImpl} object
     *
     * @param beanManager
     *        CDI's {@link BeanManager} instance
     * @param defaultConfiguration
     *        the default serdes configuration of the framework
     * @return the resolved instance
     */
    @Produces
    public TopologyConfigurationImpl configuration(BeanManager beanManager,
            DefaultTopologySerdesConfiguration defaultConfiguration) {
        // Configuration for the Topology we build:
        // Step 1: initialize the configuration
        TopologyConfigurationImpl configuration = initializeConfiguration(beanManager);
        // Step 2: apply default configuration
        defaultConfiguration.apply(configuration);
        // Step 3: if an alternative customizer is provided, then apply it (default does nothing)
        configCustomizer.fillConfiguration(configuration);
        return configuration;
    }

    private static TopologyConfigurationImpl initializeConfiguration(BeanManager beanManager) {
        Class<?> processorType = TypeUtils.reifiedProcessorType(beanManager);
        Class<?> payloadType = TypeUtils.extractPayloadType(processorType);
        if (payloadType == null || Object.class.equals(payloadType)) {
            throw new IllegalArgumentException(
                    "Could not determine payload type of Processor class " + processorType.getName());
        }
        return new TopologyConfigurationImpl(payloadType);
    }

    /**
     * Based on the topology configured, build the final {@link Topology} for Kafka Streams.
     *
     * @param configuration
     *        the configuration built which resolves serdes and state stores
     * @param kStreamProcessorSupplier
     *        this framework's {@link ProcessorSupplier} responsible for decoration
     * @return the final {@link Topology}
     */
    @Produces
    public Topology topology(TopologyConfigurationImpl configuration,
            KStreamProcessorSupplier<?, ?, ?, ?> kStreamProcessorSupplier) {

        Map<String, String[]> sourceToTopicMapping = sourceToTopicsMappingBuilder.sourceToTopicsMapping();
        Map<String, String> sinkToTopicMapping = sinkToTopicMappingBuilder.sinkToTopicMapping();

        // Now we can build the Topology !
        Topology topology = new Topology();
        sourceToTopicMapping.forEach((String source, String[] topics) -> topology.addSource(source,
                new StringDeserializer(), configuration.getSourceValueSerde().deserializer(), topics));
        topology.addProcessor(PROCESSOR_NAME,
                kStreamProcessorSupplier,
                sourceToTopicMapping.keySet().toArray(new String[] {}));
        sinkToTopicMapping.forEach((String sink, String topic) -> topology.addSink(sink, topic, new StringSerializer(),
                configuration.getSinkValueSerializer(), PROCESSOR_NAME));
        if (kStreamsProcessorConfig.dlq().topic().isPresent()) {
            topology.addSink(DLQ_SINK_NAME, kStreamsProcessorConfig.dlq().topic().get(), new StringSerializer(),
                    configuration.getSourceValueSerde().serializer(), PROCESSOR_NAME);
        }

        configuration.getStoreConfigurations()
                .forEach(storeConfiguration -> topology.addStateStore(storeConfiguration.getStoreBuilder(), PROCESSOR_NAME));
        return topology;
    }
}
