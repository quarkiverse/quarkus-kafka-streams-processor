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

import jakarta.enterprise.inject.spi.CDI;

import org.apache.kafka.streams.errors.DeserializationExceptionHandler;

import io.quarkus.runtime.annotations.RegisterForReflection;
import lombok.AllArgsConstructor;
import lombok.experimental.Delegate;

/**
 * <p>
 * This class is responsible of handling exception thrown when KafkaStreams cannot deserialize a message from in an
 * upstream topic.
 * </p>
 * It uses {@link LogAndSendToDlqExceptionHandlerDelegate}, which is a CDI Bean, to delegate the responsibility of
 * producing the message into a Dead Letter Queue (DLQ) topic
 */
@AllArgsConstructor
@RegisterForReflection
public class LogAndSendToDlqExceptionHandler implements DeserializationExceptionHandler {
    /**
     * Delegate object to profit from CDI dependency injection
     */
    @Delegate
    private final DeserializationExceptionHandler delegate;

    /**
     * No args constructor used by KafkaStreams to instantiate based on the provided configuration.
     */
    public LogAndSendToDlqExceptionHandler() {
        this.delegate = CDI.current().select(LogAndSendToDlqExceptionHandlerDelegate.class).get();
    }
}
