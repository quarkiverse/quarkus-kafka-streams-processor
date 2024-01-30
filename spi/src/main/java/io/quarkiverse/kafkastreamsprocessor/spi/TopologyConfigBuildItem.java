package io.quarkiverse.kafkastreamsprocessor.spi;

import java.util.Map;

import io.quarkus.builder.item.SimpleBuildItem;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * A build item that gives access to the topology configured in the extension
 */
@Getter
@AllArgsConstructor
public final class TopologyConfigBuildItem extends SimpleBuildItem {
    /**
     * Mapping between the Source in the configuration and the associated topics
     */
    Map<String, String[]> sourceToTopicsMapping;
    /**
     * Mapping between the Sinks in the configuration and the associated topics
     */
    Map<String, String> sinkToTopicMapping;
}
