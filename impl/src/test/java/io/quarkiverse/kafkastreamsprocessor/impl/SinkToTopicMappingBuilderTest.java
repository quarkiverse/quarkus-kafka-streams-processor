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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.quarkiverse.kafkastreamsprocessor.spi.properties.KStreamsProcessorConfig;
import io.quarkiverse.kafkastreamsprocessor.spi.properties.OutputConfig;
import io.quarkiverse.kafkastreamsprocessor.spi.properties.SinkConfig;

@ExtendWith(MockitoExtension.class)
public class SinkToTopicMappingBuilderTest {
    @Mock
    KStreamsProcessorConfig extensionConfiguration;

    @Mock
    OutputConfig outputConfig;

    Map<String, SinkConfig> sinks = new HashMap<>();

    SinkToTopicMappingBuilder sinkConfiguration;

    @BeforeEach
    void setUp() {
        sinkConfiguration = new SinkToTopicMappingBuilder(extensionConfiguration);
        when(extensionConfiguration.output()).thenReturn(outputConfig);
        when(outputConfig.sinks()).thenReturn(sinks);
    }

    private void configureSink(String name, String topic) {
        SinkConfig sinkConfig = mock(SinkConfig.class);
        when(sinkConfig.topic()).thenReturn(topic);
        sinks.put(name, sinkConfig);
    }

  private void configureTopic(String topic) {
    when(outputConfig.topic()).thenReturn(Optional.of(topic));
  }

    @Test
    void sinkToTopicMapping_whenSingleSink_shouldGenerateMapping() {
        configureSink("ping", "ping-topic");

        Map<String, String> sinkToTopicMapping = sinkConfiguration.sinkToTopicMapping();

        Assertions.assertEquals(Map.of("ping", "ping-topic"), sinkToTopicMapping);
    }

    @Test
    void sinkToTopicMapping_whenMultipleSinks_shouldGenerateMapping() {
        configureSink("pong", "pong-topic");
        configureSink("ping", "ping-topic");
        configureSink("pang", "pang-topic");

        Map<String, String> sinkToTopicMapping = sinkConfiguration.sinkToTopicMapping();

        Assertions.assertEquals(Map.of("ping", "ping-topic", "pong", "pong-topic", "pang", "pang-topic"),
                sinkToTopicMapping);
    }

    @Test
    void sinkToTopicMapping_whenSinkWitDash_shouldGenerateMapping() {
        configureSink("my-channel", "my-topic");

        Map<String, String> sinkToTopicMapping = sinkConfiguration.sinkToTopicMapping();

        Assertions.assertEquals(Map.of("my-channel", "my-topic"), sinkToTopicMapping);

    }

    @Test
    void sinkToTopicMapping_whenNoSinkButOutputTopic_shouldGenerateMapping() {
        configureTopic("ping-topic");

        Map<String, String> sinkToTopicMapping = sinkConfiguration.sinkToTopicMapping();

        Assertions.assertEquals(Map.of("emitter-channel", "ping-topic"), sinkToTopicMapping);
    }

    @Test
    void sinkToTopicMapping_whenNoSinkAndNoOutputTopic_shouldGenerateEmptyMapping() {
        Map<String, String> sinkToTopicMapping = sinkConfiguration.sinkToTopicMapping();

        Assertions.assertEquals(Map.of(), sinkToTopicMapping);
    }

}
