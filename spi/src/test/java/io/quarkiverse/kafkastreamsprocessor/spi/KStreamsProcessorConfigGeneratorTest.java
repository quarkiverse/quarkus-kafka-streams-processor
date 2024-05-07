package io.quarkiverse.kafkastreamsprocessor.spi;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
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

import io.quarkiverse.kafkastreamsprocessor.spi.properties.KStreamsProcessorConfig;

@ExtendWith(MockitoExtension.class)
class KStreamsProcessorConfigGeneratorTest {
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
    void singleSink() {
        mockProperties(Map.of("something.else", "value",
                "kafkastreamsprocessor.output.sinks.ping.topic", "ping-topic"));

        KStreamsProcessorConfig kstreamsProcessorConfig = KStreamsProcessorConfigGenerator.buildConfig(config);

        assertThat(kstreamsProcessorConfig.output().sinks(), hasKey("ping"));
        assertThat(kstreamsProcessorConfig.output().sinks().get("ping").topic(), equalTo("ping-topic"));
    }

    @Test
    void multipleSinks() {
        mockProperties(Map.of("kafkastreamsprocessor.output.sinks.pong.topic", "pong-topic",
                "kafkastreamsprocessor.output.sinks.ping.topic", "ping-topic",
                "kafkastreamsprocessor.output.sinks.pang.topic", "pang-topic"));

        KStreamsProcessorConfig kstreamsProcessorConfig = KStreamsProcessorConfigGenerator.buildConfig(config);

        assertThat(kstreamsProcessorConfig.output().sinks(), allOf(hasKey("ping"), hasKey("pong"), hasKey("pang")));
        assertThat(kstreamsProcessorConfig.output().sinks().get("ping").topic(), equalTo("ping-topic"));
        assertThat(kstreamsProcessorConfig.output().sinks().get("pong").topic(), equalTo("pong-topic"));
        assertThat(kstreamsProcessorConfig.output().sinks().get("pang").topic(), equalTo("pang-topic"));
    }

    @Test
    void sinkWithDash() {
        mockProperties(Map.of("kafkastreamsprocessor.output.sinks.my-channel.topic", "my-topic"));

        KStreamsProcessorConfig kstreamsProcessorConfig = KStreamsProcessorConfigGenerator.buildConfig(config);

        assertThat(kstreamsProcessorConfig.output().sinks(), hasKey("my-channel"));
        assertThat(kstreamsProcessorConfig.output().sinks().get("my-channel").topic(), equalTo("my-topic"));
    }

    @Test
    void noSinkButOutputTopic() {
        mockProperties(Map.of("kafkaprocessor.output.topic", "ping-topic",
                "something.else", "value",
                "kafkastreamsprocessor.output.sinks.incorrect", "missing-dot-topic",
                "kafkastreamsprocessor.output.sinks.incorrect.notopic", "invalid-suffix"));

        KStreamsProcessorConfig generatedConfig = KStreamsProcessorConfigGenerator.buildConfig(config);

        assertThat(generatedConfig.output().sinks(), anEmptyMap());
        assertThat(generatedConfig.output().topic().isEmpty(), equalTo(true));
    }

    @Test
    void sinkWithDot() {
        mockProperties(Map.of("kafkastreamsprocessor.output.sinks..incorrect.topic", "too-many-dots"));

        assertThrows(IllegalStateException.class, () -> KStreamsProcessorConfigGenerator.buildConfig(config));
    }

    @Test
    void noSinkAndNoOutputTopic() {
        mockProperties(Map.of("something.else", "value",
                "kafkastreamsprocessor.output.sinks.incorrect", "missing-dot-topic",
                "kafkastreamsprocessor.output.sinks.incorrect.notopic", "invalid-suffix"));

        KStreamsProcessorConfig generatedConfig = KStreamsProcessorConfigGenerator.buildConfig(config);

        assertThat(generatedConfig.output().sinks(), anEmptyMap());
        assertThat(generatedConfig.output().topic().isEmpty(), equalTo(true));
    }

    @Test
    void whenSingleSource() {
        mockProperties(Map.of("something.else", "value",
                "kafkastreamsprocessor.input.sources.ping.topics", "ping-topic,other-ping"));

        KStreamsProcessorConfig kstreamsProcessorConfig = KStreamsProcessorConfigGenerator.buildConfig(config);

        assertThat(kstreamsProcessorConfig.input().sources(), hasKey("ping"));
        assertThat(kstreamsProcessorConfig.input().sources().get("ping").topics(), contains("ping-topic", "other-ping"));
    }

    @Test
    void whenMultipleSources() {
        mockProperties(Map.of("kafkastreamsprocessor.input.sources.pong.topics", "pong-topic",
                "kafkastreamsprocessor.input.sources.ping.topics", "ping-topic",
                "kafkastreamsprocessor.input.sources.pang.topics", "pang-topic"));

        KStreamsProcessorConfig kstreamsProcessorConfig = KStreamsProcessorConfigGenerator.buildConfig(config);

        assertThat(kstreamsProcessorConfig.input().sources(), allOf(hasKey("ping"), hasKey("pong"), hasKey("pang")));
        assertThat(kstreamsProcessorConfig.input().sources().get("ping").topics(), contains("ping-topic"));
        assertThat(kstreamsProcessorConfig.input().sources().get("pong").topics(), contains("pong-topic"));
        assertThat(kstreamsProcessorConfig.input().sources().get("pang").topics(), contains("pang-topic"));
    }

    @Test
    void whenSourceWitDash() {
        mockProperties(Map.of("kafkastreamsprocessor.input.sources.my-channel.topics", "my-topic"));

        KStreamsProcessorConfig kstreamsProcessorConfig = KStreamsProcessorConfigGenerator.buildConfig(config);

        assertThat(kstreamsProcessorConfig.input().sources(), hasKey("my-channel"));
        assertThat(kstreamsProcessorConfig.input().sources().get("my-channel").topics(), contains("my-topic"));

    }

    @Test
    void whenNoSourceButInputTopic() {
        mockProperties(Map.of("kafkastreamsprocessor.input.topic", "ping-topic",
                "something.else", "value",
                "kafkastreamsprocessor.input.sources.incorrect", "missing-dot-topic",
                "kafkastreamsprocessor.input.sources.incorrect.notopic", "invalid-suffix"));

        KStreamsProcessorConfig generatedConfig = KStreamsProcessorConfigGenerator.buildConfig(config);

        assertThat(generatedConfig.input().sources(), anEmptyMap());
        assertThat(generatedConfig.input().topic().get(), equalTo("ping-topic"));
        assertThat(generatedConfig.input().topics().isEmpty(), equalTo(true));
    }

    @Test
    void sourceWithDot() {
        mockProperties(Map.of("kafkastreamsprocessor.input.sources..incorrect.topics", "too-many-dots"));

        assertThrows(IllegalStateException.class, () -> KStreamsProcessorConfigGenerator.buildConfig(config));
    }

    @Test
    void whenNoSourceAndNoInputTopic() {
        mockProperties(Map.of("something.else", "value",
                "kafkastreamsprocessor.input.sources.incorrect", "missing-dot-topic",
                "kafkastreamsprocessor.input.sources.incorrect.notopic", "invalid-suffix"));

        KStreamsProcessorConfig generatedConfig = KStreamsProcessorConfigGenerator.buildConfig(config);

        assertThat(generatedConfig.input().sources(), anEmptyMap());
        assertThat(generatedConfig.input().topic().isEmpty(), equalTo(true));
        assertThat(generatedConfig.input().topics().isEmpty(), equalTo(true));
    }

}
