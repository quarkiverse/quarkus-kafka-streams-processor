package io.quarkiverse.kafkastreamsprocessor.runtime;

import io.quarkiverse.kafkastreamsprocessor.spi.properties.KStreamsProcessorConfig;
import io.quarkus.runtime.annotations.ConfigRoot;
import io.smallrye.config.ConfigMapping;

/**
 * Interface to trick the injection of the {@link KStreamsProcessorConfig} in the deployment module
 */
@ConfigMapping(prefix = "kafkastreamsprocessor")
@ConfigRoot
public interface KStreamsProcessorConfigRuntime extends KStreamsProcessorConfig {
}
