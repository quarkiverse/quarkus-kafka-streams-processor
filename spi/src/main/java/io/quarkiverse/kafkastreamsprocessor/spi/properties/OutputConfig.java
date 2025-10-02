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

import java.net.URI;
import java.util.Map;
import java.util.Optional;

import io.cloudevents.SpecVersion;
import io.smallrye.config.WithDefault;

/**
 * Configuration related to the messages produced by the Processor
 */
public interface OutputConfig {
    /**
     * The processor is mono-output, we designate one topic
     */
    Optional<String> topic();

    /**
     * In case the application has to output to multiple topics, this entry should be used to associate sink names with
     * topics.
     */
    Map<String, SinkConfig> sinks();

    /**
     * Whether cloud events are used on the output
     */
    @WithDefault("false")
    boolean isCloudEvent();

    /**
     * Allows to inject custom configuration for the CloudEventSerializer.
     * <p>
     * The potential values are documented in the cloudevents Java SDK (linked thereafter) and in the
     * <a href="https://docs.quarkiverse.io/quarkus-kafka-streams-processor/dev/index.html#_cloudevent_support">
     * quarkus-kafka-streams-processor userguide</a>
     *
     * @see <a href="https://www.javadoc.io/doc/io.cloudevents/cloudevents-kafka/latest/index.html">CloudEventSerializer
     *      Javadoc</a>
     */
    Map<String, String> cloudEventSerializerConfig();

    /**
     * Allows to define the type field of the CloudEvent for all the configured sinks.
     * <p>
     * It is used only if {@link OutputConfig#isCloudEvent()} is <code>true</code>.
     *
     * @return a type string that will be set in all CloudEvent produced
     */
    Optional<String> cloudEventsType();

    /**
     * Allows to define the source field of the CloudEvent for all the configured sinks.
     * <p>
     * It is used only if {@link OutputConfig#isCloudEvent()} is <code>true</code>.
     *
     * @return a source string that will be set in all CloudEvent produced
     */
    Optional<URI> cloudEventsSource();

    /**
     * Version of the CloudEvents spec to use
     *
     * @return the version fo the cloud events spec to use
     */
    @WithDefault("V1")
    SpecVersion cloudEventsSpecVersion();
}
