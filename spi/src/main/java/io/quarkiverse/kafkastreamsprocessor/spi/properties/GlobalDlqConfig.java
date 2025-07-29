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

import java.util.Optional;

import io.smallrye.config.WithDefault;

/**
 * Configuration related to the global dead-letter-queue
 */
public interface GlobalDlqConfig {
    /**
     * Global Dead letter Queue to produce error messages not managed by the application
     */
    Optional<String> topic();

    /**
     * Global Dead letter Queue maximum message size in bytes.
     * Default is 2147483647 bytes, i.e. about 2GB.
     */
    @WithDefault("2147483647")
    int maxMessageSize();
}
