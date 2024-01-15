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

import io.quarkiverse.kafkastreamsprocessor.api.SinkToTopicMappingBuilder;
import io.quarkiverse.kafkastreamsprocessor.api.properties.KStreamsProcessorConfig;
import io.quarkiverse.kafkastreamsprocessor.api.properties.SinkConfig;
import lombok.extern.slf4j.Slf4j;

/**
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
public class DefaultSinkToTopicMappingBuilder implements SinkToTopicMappingBuilder {
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
    public DefaultSinkToTopicMappingBuilder(KStreamsProcessorConfig extensionConfiguration) {
        this.extensionConfiguration = extensionConfiguration;
    }

    /**
     * {@inheritDoc}
     */
    @Override
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
