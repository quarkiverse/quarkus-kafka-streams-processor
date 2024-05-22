package io.quarkiverse.kafkastreamsprocessor.spi;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.eclipse.microprofile.config.Config;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class SinkToTopicMappingBuilderFromConfigTest {

    @Mock
    Config config;

  private void mockProperties(Map<String, String> keyValues) {
    when(config.getPropertyNames()).thenReturn(keyValues.keySet());
    keyValues.forEach((k, v) -> {
      lenient().when(config.getValue(k, String.class)).thenReturn(v);
      lenient().when(config.getOptionalValue(k, String.class)).thenReturn(Optional.of(v));
      lenient().when(config.getValues(k, String.class)).thenReturn(List.of(v.split(",")));
      lenient().when(config.getOptionalValues(k, String.class)).thenReturn(Optional.of(List.of(v.split(","))));
    });
  }

    @Test
    void sinkToTopicMapping_whenSingleSink_shouldGenerateMapping() {
        mockProperties(Map.of("something.else", "value",
                "kafkastreamsprocessor.output.sinks.ping.topic", "ping-topic"));

        Map<String, String> sinkToTopicMapping = new SinkToTopicMappingBuilder(config).sinkToTopicMapping();

        assertEquals(Map.of("ping", "ping-topic"), sinkToTopicMapping);
    }

    @Test
    void sinkToTopicMapping_whenMultipleSinks_shouldGenerateMapping() {
        mockProperties(Map.of("kafkastreamsprocessor.output.sinks.pong.topic", "pong-topic",
                "kafkastreamsprocessor.output.sinks.ping.topic", "ping-topic",
                "kafkastreamsprocessor.output.sinks.pang.topic", "pang-topic"));

        Map<String, String> sinkToTopicMapping = new SinkToTopicMappingBuilder(config).sinkToTopicMapping();

        assertEquals(Map.of("ping", "ping-topic", "pong", "pong-topic", "pang", "pang-topic"),
                sinkToTopicMapping);
    }

    @Test
    void sinkToTopicMapping_whenSinkWitDash_shouldGenerateMapping() {
        mockProperties(Map.of("kafkastreamsprocessor.output.sinks.my-channel.topic", "my-topic"));

        Map<String, String> sinkToTopicMapping = new SinkToTopicMappingBuilder(config).sinkToTopicMapping();

        assertEquals(Map.of("my-channel", "my-topic"), sinkToTopicMapping);

    }

    @Test
    void sinkToTopicMapping_whenNoSinkButOutputTopic_shouldGenerateMapping() {
        mockProperties(Map.of("kafkastreamsprocessor.output.topic", "ping-topic",
                "something.else", "value",
                "kafkastreamsprocessor.output.sinks.incorrect", "missing-dot-topic",
                "kafkastreamsprocessor.output.sinks.incorrect.notopic", "invalid-suffix"));

        Map<String, String> sinkToTopicMapping = new SinkToTopicMappingBuilder(config).sinkToTopicMapping();

        assertEquals(Map.of("emitter-channel", "ping-topic"), sinkToTopicMapping);
    }

    @Test
    void sinkWithDot() {
        mockProperties(Map.of("kafkastreamsprocessor.output.sinks..incorrect.topic", "too-many-dotsinvalid-suffix"));

        assertThrows(IllegalStateException.class, () -> new SinkToTopicMappingBuilder(config).sinkToTopicMapping());
    }

    @Test
    void sinkToTopicMapping_whenNoSinkAndNoOutputTopic_shouldGenerateEmptyMapping() {
        mockProperties(Map.of("something.else", "value",
                "kafkastreamsprocessor.output.sinks.incorrect", "missing-dot-topic",
                "kafkastreamsprocessor.output.sinks.incorrect.notopic", "invalid-suffix"));

        Map<String, String> sinkToTopicMapping = new SinkToTopicMappingBuilder(config).sinkToTopicMapping();

        assertThat(sinkToTopicMapping, anEmptyMap());
    }

}
