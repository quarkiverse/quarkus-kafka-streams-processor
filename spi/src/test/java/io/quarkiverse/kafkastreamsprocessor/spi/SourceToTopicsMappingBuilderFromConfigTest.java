package io.quarkiverse.kafkastreamsprocessor.spi;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
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
public class SourceToTopicsMappingBuilderFromConfigTest {

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
    void sourceToTopicMapping_whenSingleSource_shouldGenerateMapping() {
        mockProperties(Map.of("something.else", "value",
                "kafkastreamsprocessor.input.sources.ping.topics", "ping-topic,other-ping"));

        Map<String, String[]> sourceToTopicMapping = new SourceToTopicsMappingBuilder(config).sourceToTopicsMapping();

        Map<String, String[]> expected = Map.of("ping", new String[] { "ping-topic", "other-ping" });
        assertEquals(expected.size(), sourceToTopicMapping.size());
        expected
                .forEach((key, expectedValue) -> assertArrayEquals(expectedValue, sourceToTopicMapping.get(key)));
    }

    @Test
    void sourceToTopicMapping_whenMultipleSources_shouldGenerateMapping() {
        mockProperties(Map.of("kafkastreamsprocessor.input.sources.pong.topics", "pong-topic",
                "kafkastreamsprocessor.input.sources.ping.topics", "ping-topic",
                "kafkastreamsprocessor.input.sources.pang.topics", "pang-topic"));

        Map<String, String[]> sourceToTopicMapping = new SourceToTopicsMappingBuilder(config).sourceToTopicsMapping();

        Map<String, String[]> expected = Map.of("ping", new String[] { "ping-topic" }, "pong",
                new String[] { "pong-topic" }, "pang", new String[] { "pang-topic" });
        assertEquals(expected.size(), sourceToTopicMapping.size());
        expected
                .forEach((key, expectedValue) -> assertArrayEquals(expectedValue, sourceToTopicMapping.get(key)));
    }

    @Test
    void sourceToTopicMapping_whenSourceWitDash_shouldGenerateMapping() {
        mockProperties(Map.of("kafkastreamsprocessor.input.sources.my-channel.topics", "my-topic"));

        Map<String, String[]> sourceToTopicMapping = new SourceToTopicsMappingBuilder(config).sourceToTopicsMapping();

        Map<String, String[]> expected = Map.of("my-channel", new String[] { "my-topic" });
        assertEquals(expected.size(), sourceToTopicMapping.size());
        expected
                .forEach((key, expectedValue) -> assertArrayEquals(expectedValue, sourceToTopicMapping.get(key)));

    }

    @Test
    void sourceToTopicMapping_whenNoSourceButInputTopic_shouldGenerateMapping() {
        mockProperties(Map.of("kafkastreamsprocessor.input.topic", "ping-topic",
                "something.else", "value",
                "kafkastreamsprocessor.input.sources.incorrect", "missing-dot-topic",
                "kafkastreamsprocessor.input.sources.incorrect.notopic", "invalid-suffix"));

        Map<String, String[]> map = new SourceToTopicsMappingBuilder(config).sourceToTopicsMapping();

        assertThat(map, allOf(aMapWithSize(1), hasEntry("receiver-channel", new String[] { "ping-topic" })));
    }

    @Test
    void sourceWithDot() {
        mockProperties(Map.of("kafkastreamsprocessor.input.sources..incorrect.topics", "too-many-dots"));

        assertThrows(IllegalStateException.class, () -> new SourceToTopicsMappingBuilder(config).sourceToTopicsMapping());
    }

    @Test
    void sourceToTopicMapping_whenNoSourceAndNoInputTopic_shouldGenerateEmptyMapping() {
        mockProperties(Map.of("something.else", "value",
                "kafkastreamsprocessor.input.sources.incorrect", "missing-dot-topic",
                "kafkastreamsprocessor.input.sources.incorrect.notopic", "invalid-suffix"));

        Map<String, String[]> map = new SourceToTopicsMappingBuilder(config).sourceToTopicsMapping();

        assertThat(map, anEmptyMap());
    }

}
