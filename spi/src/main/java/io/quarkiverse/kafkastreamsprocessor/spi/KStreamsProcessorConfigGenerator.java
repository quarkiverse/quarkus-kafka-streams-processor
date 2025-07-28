package io.quarkiverse.kafkastreamsprocessor.spi;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.eclipse.microprofile.config.Config;

import io.quarkiverse.kafkastreamsprocessor.spi.properties.DlqConfig;
import io.quarkiverse.kafkastreamsprocessor.spi.properties.GlobalDlqConfig;
import io.quarkiverse.kafkastreamsprocessor.spi.properties.GlobalStateStoreConfig;
import io.quarkiverse.kafkastreamsprocessor.spi.properties.InputConfig;
import io.quarkiverse.kafkastreamsprocessor.spi.properties.KStreamsProcessorConfig;
import io.quarkiverse.kafkastreamsprocessor.spi.properties.OutputConfig;
import io.quarkiverse.kafkastreamsprocessor.spi.properties.RetryConfig;
import io.quarkiverse.kafkastreamsprocessor.spi.properties.SinkConfig;
import io.quarkiverse.kafkastreamsprocessor.spi.properties.SourceConfig;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;

class KStreamsProcessorConfigGenerator {

    private static final Pattern P_SOURCE = Pattern.compile("kafkastreamsprocessor\\.input\\.sources\\.(.*)\\.topics");

    private static final Pattern P_SINK = Pattern.compile("kafkastreamsprocessor\\.output\\.sinks\\.(.*)\\.topic");

    static KStreamsProcessorConfig buildConfig(Config config) {
        InputConfigImpl input = new InputConfigImpl(config.getOptionalValue("kafkastreamsprocessor.input.topic", String.class),
                config.getOptionalValues("kafkastreamsprocessor.input.topics", String.class));
        fillSources(config, input);
        OutputConfigImpl output = new OutputConfigImpl(
                config.getOptionalValue("kafkastreamsprocessor.output.topic", String.class));
        fillSinks(config, output);
        return new KStreamsProcessorConfigImpl(input, output);
    }

    private static void fillSources(Config config, InputConfigImpl input) {
        for (String property : config.getPropertyNames()) {
            Matcher matcher = P_SOURCE.matcher(property);
            if (matcher.matches()) {
                String sourceName = matcher.group(1);
                String topics = config.getValue(property, String.class);
                SourceConfig sourceConfig = new SourceConfigImpl(List.of(topics.split(",")));
                if (sourceName.contains(".")) {
                    throw new IllegalStateException("Parsed source name has a dot: " + sourceName);
                }
                input.sources().put(sourceName, sourceConfig);
            }
        }
    }

    private static void fillSinks(Config config, OutputConfigImpl output) {
        for (String property : config.getPropertyNames()) {
            Matcher matcher = P_SINK.matcher(property);
            if (matcher.matches()) {
                String sinkName = matcher.group(1);
                String topic = config.getValue(property, String.class);
                SinkConfig sinkConfig = new SinkConfigImpl(topic);
                if (sinkName.contains(".")) {
                    throw new IllegalStateException("Parsed sink name has a dot: " + sinkName);
                }
                output.sinks().put(sinkName, sinkConfig);
            }
        }
    }

    @RequiredArgsConstructor
    @Getter
    @Accessors(fluent = true)
    private static class KStreamsProcessorConfigImpl implements KStreamsProcessorConfig {
        private final InputConfig input;
        private final OutputConfig output;
        private final DlqConfig dlq = null;
        private final GlobalDlqConfig globalDlq = null;
        private final Map<String, GlobalStateStoreConfig> globalStores = null;
        private final String errorStrategy = "";
        private final RetryConfig retry = null;
    }

    @RequiredArgsConstructor
    @Getter
    @Accessors(fluent = true)
    private static class InputConfigImpl implements InputConfig {
        private final Optional<String> topic;

        private final Optional<List<String>> topics;

        private final Map<String, SourceConfig> sources = new HashMap<>();
    }

    @RequiredArgsConstructor
    @Getter
    @Accessors(fluent = true)
    private static class SourceConfigImpl implements SourceConfig {
        private final List<String> topics;
    }

    @RequiredArgsConstructor
    @Getter
    @Accessors(fluent = true)
    private static class OutputConfigImpl implements OutputConfig {
        private final Optional<String> topic;

        private final Map<String, SinkConfig> sinks = new HashMap<>();
    }

    @RequiredArgsConstructor
    @Getter
    @Accessors(fluent = true)
    private static class SinkConfigImpl implements SinkConfig {
        private final String topic;
    }

}
