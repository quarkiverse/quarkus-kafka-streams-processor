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

import java.util.Map;
import java.util.Optional;

import io.quarkiverse.kafkastreamsprocessor.api.Processor;

/**
 * Regroups the configuration related to the messages in output of the {@link Processor}.
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
}
