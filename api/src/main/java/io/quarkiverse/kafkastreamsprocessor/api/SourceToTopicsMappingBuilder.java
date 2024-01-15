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
package io.quarkiverse.kafkastreamsprocessor.api;

import java.util.Map;

/**
 * Object to inject to get access to the resolved mapping between input topics and sources for a multi input processor, using
 * the
 * conventions set up by the framework based on config properties like:
 *
 * <pre>
 * kafkastreamsprocessor.input.sources.pong.topics=pong-events
 * kafkastreamsprocessor.input.sources.pang.topics=pang-events,ping-events
 * </pre>
 * <p>
 * Where:
 * </p>
 * <ul>
 * <li>pong and pang are the sources</li>
 * <li>ping-events, pong-events and pang-events the Kafka topics</li>
 * </ul>
 */
public interface SourceToTopicsMappingBuilder {
    /**
     * Default source name created by KafkaStreams if no source is configured manually
     */
    String DEFAULT_SOURCE_NAME = "receiver-channel";

    /**
     * Looks at the configuration and extracts from it the mapping from the source to the Kafka topic(s).
     * <p>
     * This method is exposed so you can do any kind of technical postprocessing based on the Kafka topic and the source
     * names.
     * </p>
     *
     * @return a map with keys the sink names and values the corresponding list of Kafka topic names
     */
    Map<String, String[]> sourceToTopicsMapping();
}
