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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.quarkiverse.kafkastreamsprocessor.spi.properties.KStreamsProcessorConfig;
import io.quarkiverse.kafkastreamsprocessor.spi.properties.SinkConfig;
import lombok.extern.slf4j.Slf4j;

/**
 * Object to inject to get access to the resolved mapping between sink and topic for a multi output processor, using the
 * conventions set up by the framework based on config properties like:
 *
 * <pre>
 * kafkastreamsprocessor.output.sinks.pong.topic=pong-events
 * kafkastreamsprocessor.output.sinks.pang.topic=pang-events
 * </pre>
 * <p>
 * Where:
 * </p>
 * <ul>
 * <li>pong and pang are the sinks</li>
 * <li>pong-events and pang-events the Kafka topics</li>
 * </ul>
 *
 * Multi-output topic configuration.
 * <p>
 * Inspired by <a href=
 * "https://github.com/smallrye/smallrye-reactive-messaging/blob/master/smallrye-reactive-messagingprovider/src/main/java/io/smallrye/reactive/messaging/impl/ConfiguredChannelFactory.java">smallrye-reactive-messaging
 * ConfiguredChannelFactory</a>
 * </p>
 * <p>
 * Example of usage in the multioutput integration test.
 * </p>
 */
@ApplicationScoped
@Slf4j
public class SinkToTopicMappingBuilder {
    /**
     * Default sink name created by KafkaStreams if no sink is configured manually
     */
    String DEFAULT_SINK_NAME = "emitter-channel";
    /**
     * Configuration object of the extension
     */
    private final KStreamsProcessorConfig extensionConfiguration;

    /**
     * Injection constructor
     *
     * @param extensionConfiguration
     *        Configuration object of the extension
     */
    @Inject
    public SinkToTopicMappingBuilder(KStreamsProcessorConfig extensionConfiguration) {
        this.extensionConfiguration = extensionConfiguration;
    }

    /**
     * Looks at the configuration and extracts from it the mapping from the sink to the Kafka topic.
     * <p>
     * This method is exposed so you can do any kind of technical postprocessing based on the Kafka topic and the sink
     * names.
     * </p>
     *
     * @return a map with keys the sink names and values the corresponding Kafka topic name
     */
    public Map<String, String> sinkToTopicMapping() {
        // Extract topic name for each sink if any has been configured
        Map<String, String> sinkToTopicMapping = buildMapping();

        // Backward compatibility
        if (sinkToTopicMapping.isEmpty()) {
            Optional<String> singleOutputTopic = extensionConfiguration.output().topic();
            if (singleOutputTopic.isPresent()) {
                return Map.of(DEFAULT_SINK_NAME, singleOutputTopic.get());
            }
        }

        return sinkToTopicMapping;
    }

    private Map<String, String> buildMapping() {
        Map<String, String> sinkToTopicMapping = new HashMap<>();
        for (Map.Entry<String, SinkConfig> sinkEntry : extensionConfiguration.output().sinks().entrySet()) {
            String topicName = sinkEntry.getValue().topic();
            if (topicName != null && !topicName.isEmpty()) {
                sinkToTopicMapping.put(sinkEntry.getKey(), topicName);
            }
        }
        return sinkToTopicMapping;
    }

}
