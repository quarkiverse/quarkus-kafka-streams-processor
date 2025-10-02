package io.quarkiverse.kafkastreamsprocessor.spi;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.eclipse.microprofile.config.Config;

import io.cloudevents.SpecVersion;
import io.quarkiverse.kafkastreamsprocessor.spi.properties.DlqConfig;
import io.quarkiverse.kafkastreamsprocessor.spi.properties.GlobalDlqConfig;
import io.quarkiverse.kafkastreamsprocessor.spi.properties.GlobalStateStoreConfig;
import io.quarkiverse.kafkastreamsprocessor.spi.properties.InputConfig;
import io.quarkiverse.kafkastreamsprocessor.spi.properties.KStreamsProcessorConfig;
import io.quarkiverse.kafkastreamsprocessor.spi.properties.OutputConfig;
import io.quarkiverse.kafkastreamsprocessor.spi.properties.RetryConfig;
import io.quarkiverse.kafkastreamsprocessor.spi.properties.SinkConfig;
import io.quarkiverse.kafkastreamsprocessor.spi.properties.SourceConfig;

class KStreamsProcessorConfigGenerator {

    private static final Pattern P_SOURCE = Pattern.compile("kafkastreamsprocessor\\.input\\.sources\\.(.*)\\.topics");

    private static final Pattern P_SINK = Pattern.compile("kafkastreamsprocessor\\.output\\.sinks\\.(.*)\\.topic");

    static KStreamsProcessorConfig buildConfig(Config config) {
        InputConfigImpl input = new InputConfigImpl();
        input.setTopic(config.getOptionalValue("kafkastreamsprocessor.input.topic", String.class));
        input.setTopics(config.getOptionalValues("kafkastreamsprocessor.input.topics", String.class));
        fillSources(config, input);
        OutputConfigImpl output = new OutputConfigImpl();
        output.setTopic(config.getOptionalValue("kafkastreamsprocessor.output.topic", String.class));
        fillSinks(config, output);
        KStreamsProcessorConfigImpl kStreamsProcessorConfig = new KStreamsProcessorConfigImpl();
        kStreamsProcessorConfig.setInput(input);
        kStreamsProcessorConfig.setOutput(output);
        return kStreamsProcessorConfig;
    }

    private static void fillSources(Config config, InputConfigImpl input) {
        Map<String, SourceConfig> sources = new HashMap<>();
        for (String property : config.getPropertyNames()) {
            Matcher matcher = P_SOURCE.matcher(property);
            if (matcher.matches()) {
                String sourceName = matcher.group(1);
                String topics = config.getValue(property, String.class);
                SourceConfigImpl sourceConfig = new SourceConfigImpl();
                sourceConfig.setTopics(List.of(topics.split(",")));
                if (sourceName.contains(".")) {
                    throw new IllegalStateException("Parsed source name has a dot: " + sourceName);
                }
                sources.put(sourceName, sourceConfig);
            }
        }
        input.setSources(sources);
    }

    private static void fillSinks(Config config, OutputConfigImpl output) {
        Map<String, SinkConfig> sinks = new HashMap<>();
        for (String property : config.getPropertyNames()) {
            Matcher matcher = P_SINK.matcher(property);
            if (matcher.matches()) {
                String sinkName = matcher.group(1);
                String topic = config.getValue(property, String.class);
                SinkConfigImpl sinkConfig = new SinkConfigImpl();
                sinkConfig.setTopic(topic);
                if (sinkName.contains(".")) {
                    throw new IllegalStateException("Parsed sink name has a dot: " + sinkName);
                }
                sinks.put(sinkName, sinkConfig);
            }
        }
        output.setSinks(sinks);
    }

    private static class KStreamsProcessorConfigImpl implements KStreamsProcessorConfig {
        private InputConfig input;
        private OutputConfig output;

        @Override
        public InputConfig input() {
            return input;
        }

        public void setInput(InputConfig input) {
            this.input = input;
        }

        @Override
        public OutputConfig output() {
            return output;
        }

        public void setOutput(OutputConfig output) {
            this.output = output;
        }

        @Override
        public DlqConfig dlq() {
            return null;
        }

        @Override
        public GlobalDlqConfig globalDlq() {
            return null;
        }

        @Override
        public String errorStrategy() {
            return null;
        }

        @Override
        public RetryConfig retry() {
            return null;
        }

        @Override
        public Map<String, GlobalStateStoreConfig> globalStores() {
            return Map.of();
        }
    }

    private static class InputConfigImpl implements InputConfig {
        private Optional<String> topic;

        private Optional<List<String>> topics;

        private Map<String, SourceConfig> sources;

        private boolean isCloudEvent;

        private Map<String, String> cloudEventConfig;

        @Override
        public Optional<String> topic() {
            return topic;
        }

        public void setTopic(Optional<String> topic) {
            this.topic = topic;
        }

        @Override
        public Optional<List<String>> topics() {
            return topics;
        }

        public void setTopics(Optional<List<String>> topics) {
            this.topics = topics;
        }

        @Override
        public Map<String, SourceConfig> sources() {
            return sources;
        }

        public void setSources(
                Map<String, SourceConfig> sources) {
            this.sources = sources;
        }

        @Override
        public boolean isCloudEvent() {
            return isCloudEvent;
        }

        public void setIsCloudEvent(boolean isCloudEvent) {
            this.isCloudEvent = isCloudEvent;
        }

        @Override
        public Map<String, String> cloudEventDeserializerConfig() {
            return cloudEventConfig;
        }

        public void setCloudEventConfig(Map<String, String> cloudEventConfig) {
            this.cloudEventConfig = cloudEventConfig;
        }
    }

    private static class SourceConfigImpl implements SourceConfig {
        private List<String> topics;

        @Override
        public List<String> topics() {
            return topics;
        }

        public void setTopics(List<String> topics) {
            this.topics = topics;
        }
    }

    private static class OutputConfigImpl implements OutputConfig {
        private Optional<String> topic;

        private Map<String, SinkConfig> sinks;
        private boolean isCloudEvent;

        private Map<String, String> cloudEventConfig;

        private Optional<String> cloudEventsType;

        private Optional<URI> cloudEventsSource;

        private SpecVersion cloudEventsSpecVersion;

        @Override
        public Optional<String> topic() {
            return topic;
        }

        public void setTopic(Optional<String> topic) {
            this.topic = topic;
        }

        @Override
        public Map<String, SinkConfig> sinks() {
            return sinks;
        }

        public void setSinks(Map<String, SinkConfig> sinks) {
            this.sinks = sinks;
        }

        @Override
        public boolean isCloudEvent() {
            return isCloudEvent;
        }

        public void setIsCloudEvent(boolean isCloudEvent) {
            this.isCloudEvent = this.isCloudEvent;
        }

        @Override
        public Map<String, String> cloudEventSerializerConfig() {
            return cloudEventConfig;
        }

        public void setCloudEventSerializerConfig(Map<String, String> cloudEventConfig) {
            this.cloudEventConfig = cloudEventConfig;
        }

        @Override
        public Optional<String> cloudEventsType() {
            return cloudEventsType;
        }

        public void setCloudEventsType(Optional<String> cloudEventsType) {
            this.cloudEventsType = cloudEventsType;
        }

        @Override
        public Optional<URI> cloudEventsSource() {
            return cloudEventsSource;
        }

        public void setCloudEventsSource(Optional<URI> cloudEventsSource) {
            this.cloudEventsSource = cloudEventsSource;
        }

        @Override
        public SpecVersion cloudEventsSpecVersion() {
            return cloudEventsSpecVersion;
        }

        public void setCloudEventsSpecVersion(SpecVersion cloudEventsSpecVersion) {
            this.cloudEventsSpecVersion = cloudEventsSpecVersion;
        }
    }

    private static class SinkConfigImpl implements SinkConfig {
        private String topic;

        @Override
        public String topic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }
    }

}
