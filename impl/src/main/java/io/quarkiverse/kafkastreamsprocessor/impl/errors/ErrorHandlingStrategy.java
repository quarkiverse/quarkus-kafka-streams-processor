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

import java.util.Optional;

/**
 * Constants related to Kafka error handling
 */
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

    /**
     * Tells whether microservice-specific DLQ is activated and has a dedicated topic
     *
     * @param errorStrategy
     *        the error strategy chosen by an application between <code>continue</code>, <code>dead-letter-queue</code>
     *        and <code>fail</code>, configured with <code>kafka.error.strategy</code>
     * @param dlqTopic
     *        the optional topic that is mandatory if the chosen error strategy is <code>dead-letter-queue</code>
     * @return whether DLQ mechanism is activated by the configuration or not
     */
    public static boolean shouldSendToDlq(String errorStrategy, Optional<String> dlqTopic) {
        if (ErrorHandlingStrategy.DEAD_LETTER_QUEUE.equals(errorStrategy)) {
            if (dlqTopic.isPresent()) {
                return true;
            } else {
                throw new IllegalStateException(
                        "DLQ strategy enabled but kafkastreamsprocessor.dlq.topic configuration property is missing");
            }
        }
        return false;
    }

    private ErrorHandlingStrategy() {
        // Prevent instantiation of utility class
    }
}
