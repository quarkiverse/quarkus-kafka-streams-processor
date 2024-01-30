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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.quarkiverse.kafkastreamsprocessor.spi.properties.InputConfig;
import io.quarkiverse.kafkastreamsprocessor.spi.properties.KStreamsProcessorConfig;
import io.quarkiverse.kafkastreamsprocessor.spi.properties.SourceConfig;

@ExtendWith(MockitoExtension.class)
public class SourceToTopicsMappingBuilderTest {
    @Mock
    KStreamsProcessorConfig extensionConfiguration;

    @Mock
    InputConfig inputConfig;

    Map<String, SourceConfig> sources = new HashMap<>();

    SourceToTopicsMappingBuilder sourceConfiguration;

    @BeforeEach
    void setUp() {
        sourceConfiguration = new SourceToTopicsMappingBuilder(extensionConfiguration);
        when(extensionConfiguration.input()).thenReturn(inputConfig);
        when(inputConfig.sources()).thenReturn(sources);
    }

    private void configureSource(String name, String... topics) {
        SourceConfig sourceConfig = mock(SourceConfig.class);
        when(sourceConfig.topics()).thenReturn(Arrays.asList(topics));
        sources.put(name, sourceConfig);
    }

  private void configureTopics(String... topics) {
        when(inputConfig.topics()).thenReturn(Optional.of(Arrays.asList(topics)));
    }

    @Test
    void sourceToTopicMapping_whenSingleSource_shouldGenerateMapping() {
        configureSource("ping", "ping-topic", "other-ping");

        Map<String, String[]> sourceToTopicMapping = sourceConfiguration.sourceToTopicsMapping();

        Map<String, String[]> expected = Map.of("ping", new String[] { "ping-topic", "other-ping" });
        Assertions.assertEquals(expected.size(), sourceToTopicMapping.size());
        expected
                .forEach((key, expectedValue) -> Assertions.assertArrayEquals(expectedValue, sourceToTopicMapping.get(key)));
    }

    @Test
    void sourceToTopicMapping_whenMultipleSources_shouldGenerateMapping() {
        configureSource("pong", "pong-topic");
        configureSource("ping", "ping-topic");
        configureSource("pang", "pang-topic");

        Map<String, String[]> sourceToTopicMapping = sourceConfiguration.sourceToTopicsMapping();

        Map<String, String[]> expected = Map.of("ping", new String[] { "ping-topic" }, "pong",
                new String[] { "pong-topic" }, "pang", new String[] { "pang-topic" });
        Assertions.assertEquals(expected.size(), sourceToTopicMapping.size());
        expected
                .forEach((key, expectedValue) -> Assertions.assertArrayEquals(expectedValue, sourceToTopicMapping.get(key)));
    }

    @Test
    void sourceToTopicMapping_whenSourceWitDash_shouldGenerateMapping() {
        configureSource("my-channel", "my-topic");

        Map<String, String[]> sourceToTopicMapping = sourceConfiguration.sourceToTopicsMapping();

        Map<String, String[]> expected = Map.of("my-channel", new String[] { "my-topic" });
        Assertions.assertEquals(expected.size(), sourceToTopicMapping.size());
        expected
                .forEach((key, expectedValue) -> Assertions.assertArrayEquals(expectedValue, sourceToTopicMapping.get(key)));

    }

    @Test
    void sourceToTopicMapping_whenNoSourceButInputTopic_shouldGenerateMapping() {
        configureTopics("ping-topic");

        Map<String, String[]> sourceToTopicMapping = sourceConfiguration.sourceToTopicsMapping();

        Map<String, String[]> expected = Map.of("receiver-channel", new String[] { "ping-topic" });
        Assertions.assertEquals(expected.size(), sourceToTopicMapping.size());
        expected
                .forEach((key, expectedValue) -> Assertions.assertArrayEquals(expectedValue, sourceToTopicMapping.get(key)));
    }

    @Test
    void sourceToTopicMapping_whenNoSourceAndNoInputTopic_shouldGenerateEmptyMapping() {
        Map<String, String[]> sourceToTopicMapping = sourceConfiguration.sourceToTopicsMapping();

        Assertions.assertEquals(Map.of(), sourceToTopicMapping);
    }

}
