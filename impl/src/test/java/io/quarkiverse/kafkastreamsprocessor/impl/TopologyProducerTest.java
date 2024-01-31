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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import jakarta.enterprise.inject.Instance;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.TopologyDescription.Processor;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.quarkiverse.kafkastreamsprocessor.api.configuration.ConfigurationCustomizer;
import io.quarkiverse.kafkastreamsprocessor.api.configuration.store.StoreConfiguration;
import io.quarkiverse.kafkastreamsprocessor.api.decorator.producer.ProducerOnSendInterceptor;
import io.quarkiverse.kafkastreamsprocessor.impl.configuration.TopologyConfigurationImpl;
import io.quarkiverse.kafkastreamsprocessor.spi.properties.DlqConfig;
import io.quarkiverse.kafkastreamsprocessor.spi.properties.KStreamsProcessorConfig;

@ExtendWith(MockitoExtension.class)
class TopologyProducerTest {
    @Mock
    KStreamsProcessorConfig kStreamsProcessorConfig;

    @Mock
    DlqConfig dlqConfig;

    @Mock
    SourceToTopicsMappingBuilder sourceToTopicsMappingBuilder;

    @Mock
    SinkToTopicMappingBuilder sinkToTopicMappingBuilder;

    @Mock
    Instance<ConfigurationCustomizer> configCustomizer;

    @Mock
    Instance<ProducerOnSendInterceptor> interceptors;

    KStreamProcessorSupplier processorSupplier = null;

    TopologyConfigurationImpl configuration = null;

    private static List<StoreConfiguration> buildStoreConfiguration() {
        List<StoreConfiguration> storeConfigurations = new ArrayList<>();
        // Add a key value store for indexes
        StoreBuilder<KeyValueStore<String, String>> storeBuilderPingData = Stores
                .keyValueStoreBuilder(
                        Stores.persistentKeyValueStore("ping-data"),
                        Serdes.String(),
                        Serdes.String());
        StoreBuilder<KeyValueStore<String, String>> storeBuilderPingIndexes = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("ping-indexes"),
                Serdes.String(),
                Serdes.String());
        storeConfigurations.add(new StoreConfiguration(storeBuilderPingData));
        storeConfigurations.add(new StoreConfiguration(storeBuilderPingIndexes));
        return storeConfigurations;
    }

    @BeforeEach
    public void setUp() {
        processorSupplier = mock(KStreamProcessorSupplier.class);
        when(processorSupplier.get()).thenReturn(mock(org.apache.kafka.streams.processor.api.Processor.class),
                mock(org.apache.kafka.streams.processor.api.Processor.class));
        configuration = mock(TopologyConfigurationImpl.class);
        when(configuration.getSourceValueSerde()).thenReturn(mock(Serde.class));
    }

    private TopologyProducer newTopologyProducer(
            Map<String, String[]> sourceToTopicMapping,
            Map<String, String> sinkToTopicMapping,
            String dlq) {
        when(kStreamsProcessorConfig.dlq()).thenReturn(dlqConfig);
        when(dlqConfig.topic()).thenReturn(Optional.ofNullable(dlq));
        when(sourceToTopicsMappingBuilder.sourceToTopicsMapping()).thenReturn(sourceToTopicMapping);
        when(sinkToTopicMappingBuilder.sinkToTopicMapping()).thenReturn(sinkToTopicMapping);
        TopologyProducer topologyProducer = new TopologyProducer(kStreamsProcessorConfig, configCustomizer, sourceToTopicsMappingBuilder, sinkToTopicMappingBuilder, interceptors);
        return topologyProducer;
    }

    private void verifyTopology(
            Map<String, String[]> sourceToTopicMapping,
            Map<String, String> sinkToTopicMapping,
            String dlq,
            TopologyDescription topology, Map<String, List<String>> processorsStoreMapping) {

        assertEquals(1, topology.subtopologies().size());
        TopologyDescription.Subtopology subtopology = topology.subtopologies().iterator().next();

        List<TopologyDescription.Source> sources = subtopology.nodes()
                .stream()
                .filter(node -> (node instanceof TopologyDescription.Source))
                .map(node -> (TopologyDescription.Source) node)
                .collect(Collectors.toList());
        List<TopologyDescription.Processor> processors = subtopology
                .nodes()
                .stream()
                .filter(node -> (node instanceof TopologyDescription.Processor))
                .map(node -> (TopologyDescription.Processor) node)
                .collect(Collectors.toList());
        List<TopologyDescription.Sink> sinks = subtopology.nodes()
                .stream()
                .filter(node -> (node instanceof TopologyDescription.Sink))
                .map(node -> (TopologyDescription.Sink) node)
                .collect(Collectors.toList());

        assertEquals(sourceToTopicMapping.size(), sources.size());
        assertEquals(1, processors.size());
        int expectedSinks = sinkToTopicMapping.size();
        if (dlq != null) {
            expectedSinks += 1;
        }
        assertEquals(expectedSinks, sinks.size());

        for (TopologyDescription.Source source : sources) {
            assertEquals(new HashSet<>(Arrays.asList(sourceToTopicMapping.get(source.name()))), source.topicSet());
            assertEquals(Set.of(), source.predecessors());
            assertEquals(Set.of(processors.get(0)), source.successors());
        }

        assertEquals(new HashSet<>(sources), processors.get(0).predecessors());
        assertEquals(new HashSet<>(sinks), processors.get(0).successors());

        for (TopologyDescription.Sink sink : sinks) {
            assertEquals(Set.of(processors.get(0)), sink.predecessors());
            assertEquals(Set.of(), sink.successors());

            assertEquals(sinkToTopicMapping.getOrDefault(sink.name(), dlq), sink.topic());
        }

        if (processorsStoreMapping != null) {
            processorsStoreMapping.forEach((processorName, stores) -> {
                Optional<Processor> found = processors
                        .stream()
                        .filter(processor -> processor.name().equals(processorName))
                        .findAny();
                assertTrue(found.isPresent());
                assertEquals(found.get().stores(), new HashSet<>(stores));
            });
        }

    }

    @Test
    void topology_whenMultipleOutputTopics_shouldGenerateTopology() {
        TopologyProducer topologyProducer = newTopologyProducer(
                Map.of("source", new String[] { "ping-topic" }),
                Map.of("pong", "pong-topic", "pang", "pang-topic"),
                null);

        TopologyDescription topology = topologyProducer.topology(configuration, processorSupplier).describe();

        verifyTopology(Map.of("source", new String[] { "ping-topic" }),
                Map.of("pong", "pong-topic", "pang", "pang-topic"),
                null,
                topology, null);
    }

    @Test
    void topology_whenMultipleSources_shouldGenerateTopology() {
        TopologyProducer topologyProducer = newTopologyProducer(
                Map.of("ping", new String[] { "ping-topic", "ping-topic2" }, "pang", new String[] { "pang-topic" }),
                Map.of("pong", "pong-topic"),
                null);

        TopologyDescription topology = topologyProducer.topology(configuration, processorSupplier).describe();

        verifyTopology(
                Map.of("ping", new String[] { "ping-topic", "ping-topic2" }, "pang", new String[] { "pang-topic" }),
                Map.of("pong", "pong-topic"),
                null,
                topology, null);
    }

    @Test
    void topology_whenMultipleOutputTopicsAndDLQ_shouldGenerateTopology() {
        TopologyProducer topologyProducer = newTopologyProducer(
                Map.of("source", new String[] { "ping-topic" }),
                Map.of("pong", "pong-topic", "pang", "pang-topic"),
                "local-dlq");

        TopologyDescription topology = topologyProducer.topology(configuration, processorSupplier).describe();

        verifyTopology(Map.of("source", new String[] { "ping-topic" }),
                Map.of("pong", "pong-topic", "pang", "pang-topic"),
                "local-dlq",
                topology, null);
    }

    @Test
    void topology_whenMultipleOutputTopicsAndDLQAndStores_shouldGenerateTopology() {
        TopologyProducer topologyProducer = newTopologyProducer(
                Map.of("source", new String[] { "ping-topic" }),
                Map.of("pong", "pong-topic", "pang", "pang-topic"),
                null);

        List<StoreConfiguration> storeConfigurations = buildStoreConfiguration();

        when(configuration.getStoreConfigurations()).thenReturn(storeConfigurations);

        TopologyDescription topology = topologyProducer.topology(configuration, processorSupplier).describe();

        verifyTopology(Map.of("source", new String[] { "ping-topic" }),
                Map.of("pong", "pong-topic", "pang", "pang-topic"),
                null,
                topology, Map.of("Processor", Arrays.asList("ping-indexes", "ping-data")));
    }
}
