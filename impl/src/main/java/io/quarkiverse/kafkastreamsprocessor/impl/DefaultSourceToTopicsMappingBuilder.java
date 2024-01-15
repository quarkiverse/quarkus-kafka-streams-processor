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
import java.util.List;
import java.util.Map;
import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.quarkiverse.kafkastreamsprocessor.api.SourceToTopicsMappingBuilder;
import io.quarkiverse.kafkastreamsprocessor.api.properties.KStreamsProcessorConfig;
import io.quarkiverse.kafkastreamsprocessor.api.properties.SourceConfig;
import lombok.extern.slf4j.Slf4j;

/**
 * Multi-input topic configuration Inspired by <a href=
 * "https://github.com/smallrye/smallrye-reactive-messaging/blob/main/smallrye-reactive-messaging-provider/src/main/java/io/smallrye/reactive/messaging/impl/ConfiguredChannelFactory.java">smallrye-reactive-messaging
 * ConfiguredChannelFactory</a>
 */
@ApplicationScoped
@Slf4j
public class DefaultSourceToTopicsMappingBuilder implements SourceToTopicsMappingBuilder {
    /**
     * Configuration of the extension
     */
    private final KStreamsProcessorConfig extensionConfiguration;

    /**
     * Injection constructor
     *
     * @param extensionConfiguration
     *        Configuration of the extension
     */
    @Inject
    public DefaultSourceToTopicsMappingBuilder(KStreamsProcessorConfig extensionConfiguration) {
        this.extensionConfiguration = extensionConfiguration;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, String[]> sourceToTopicsMapping() {
        // Extract topic name for each channel
        Map<String, String[]> sourceToTopicMapping = buildMapping();

        // Backward compatibility
        if (sourceToTopicMapping.isEmpty()) {
            Optional<List<String>> inputTopicList = extensionConfiguration.input().topics();
            if (inputTopicList.isPresent()) {
                return Map.of(DEFAULT_SOURCE_NAME, inputTopicList.get().toArray(new String[0]));
            }
            Optional<String> singleInputTopic = extensionConfiguration.input().topic();
            if (singleInputTopic.isPresent()) {
                return Map.of(DEFAULT_SOURCE_NAME, new String[] { singleInputTopic.get() });
            }
        }

        return sourceToTopicMapping;
    }

    private Map<String, String[]> buildMapping() {
        Map<String, String[]> sourceToTopicMapping = new HashMap<>();
        for (Map.Entry<String, SourceConfig> sourceEntry : extensionConfiguration.input().sources().entrySet()) {
            List<String> topicNames = sourceEntry.getValue().topics();
            if (topicNames != null && !topicNames.isEmpty()) {
                sourceToTopicMapping.put(sourceEntry.getKey(), topicNames.toArray(new String[0]));
            }
        }
        return sourceToTopicMapping;
    }
}
