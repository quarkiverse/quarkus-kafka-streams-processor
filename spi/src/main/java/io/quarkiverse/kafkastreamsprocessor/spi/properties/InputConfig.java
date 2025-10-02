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
package io.quarkiverse.kafkastreamsprocessor.spi.properties;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.smallrye.config.WithDefault;

/**
 * Configuration related to the messages consumed by the Processor
 */
public interface InputConfig {
    /**
     * Single topic to listen to.
     * <p>
     * If you need more than one, use {@link #topics()} or {@link #sources()}.
     */
    Optional<String> topic();

    /**
     * List of topics to listen to.
     * <p>
     * If you need only one, use {@link #topic()}
     */
    Optional<List<String>> topics();

    /**
     * Additional structure to have multiple sources define and tel for each the topic(s) to use.
     * <p>
     * Allows to regroup topics by sources in multi-input use cases.
     */
    Map<String, SourceConfig> sources();

    /**
     * Whether cloud events are used on the input
     */
    @WithDefault("false")
    boolean isCloudEvent();

    /**
     * Allows to inject custom configuration for the CloudEventDeserializer.
     * <p>
     * As of now, only one configuration property is supported: <code>cloudevents.datamapper</code> and its value must be
     * a reference to a CloudEventDataMapper, which with the String value type here, cannot be passed.
     * <p>
     * But the Cloud Events Java SDK could add other configuration properties later on, and this map makes
     * <code>quarkus-kafka-streams-processor</code> more future-proof.
     *
     * @see <a href="https://www.javadoc.io/doc/io.cloudevents/cloudevents-kafka/latest/index.html">CloudEventDeserializer
     *      Javadoc</a>
     */
    Map<String, String> cloudEventDeserializerConfig();
}
