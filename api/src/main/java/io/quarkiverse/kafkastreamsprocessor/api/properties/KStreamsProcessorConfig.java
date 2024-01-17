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

package io.quarkiverse.kafkastreamsprocessor.api.properties;

import io.quarkiverse.kafkastreamsprocessor.api.exception.RetryableException;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

@ConfigMapping(prefix = "kafkastreamsprocessor")
public interface KStreamsProcessorConfig {
    /**
     * The Kafka topics for incoming messages
     */
    InputConfig input();

    /**
     * The Kafka topics for outgoing messages
     */
    OutputConfig output();

    /**
     * Dead letter Queue name
     */
    DlqConfig dlq();

    /**
     * Global Dead letter Queue config
     */
    GlobalDlqConfig globalDlq();

    /**
     * Kafka error handling strategy
     */
    @WithDefault("continue")
    String errorStrategy();

    /**
     * All configuration related to the RetryDecorator and reprocessing a record when a {@link RetryableException} has
     * been caught
     */
    RetryConfig retry();
}
