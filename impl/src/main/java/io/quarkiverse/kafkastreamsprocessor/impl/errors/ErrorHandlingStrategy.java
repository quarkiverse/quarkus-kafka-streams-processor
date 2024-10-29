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
package io.quarkiverse.kafkastreamsprocessor.impl.errors;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import io.quarkiverse.kafkastreamsprocessor.spi.properties.KStreamsProcessorConfig;

/**
 * Constants related to Kafka error handling
 */
@ApplicationScoped
public class ErrorHandlingStrategy {
    /**
     * Configuration property to check for the Kafka error handling strategy
     */
    public static final String CONFIG_PROPERTY = "kafka.error.strategy";

    /**
     * Default strategy: drop the message and continue processing
     */
    public static final String CONTINUE = "continue";

    /**
     * DLQ strategy: send the message to the DLQ and continue processing
     */
    public static final String DEAD_LETTER_QUEUE = "dead-letter-queue";

    /**
     * Fail strategy: fail and stop processing more message
     */
    public static final String FAIL = "fail";

    private final String errorStrategy;

    private final KStreamsProcessorConfig kStreamsProcessorConfig;

    @Inject
    public ErrorHandlingStrategy(@ConfigProperty(name = CONFIG_PROPERTY, defaultValue = CONTINUE) String errorStrategy,
            KStreamsProcessorConfig config) {
        this.errorStrategy = errorStrategy;
        this.kStreamsProcessorConfig = config;
    }

    /**
     * Tells whether microservice-specific DLQ is activated and has a dedicated topic
     *
     * @return whether DLQ mechanism is activated by the configuration or not
     */
    public boolean shouldSendToDlq() {
        if (DEAD_LETTER_QUEUE.equals(errorStrategy)) {
            if (kStreamsProcessorConfig.dlq().topic().isPresent()) {
                return true;
            } else {
                throw new IllegalStateException("DLQ strategy enabled but dlq.topic configuration property is missing");
            }
        }
        return false;
    }
}
