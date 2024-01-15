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

import org.apache.kafka.streams.errors.ProductionExceptionHandler;

import io.quarkus.runtime.annotations.RegisterForReflection;
import lombok.AllArgsConstructor;
import lombok.experimental.Delegate;

/**
 * <p>
 * This class is responsible of handling exception thrown when KafkaStreams cannot produce a message in any downstream
 * topics.
 * </p>
 * It uses {@link GlobalDLQProductionExceptionHandlerDelegate}, which is a CDI Bean, to delegate the responsibility of
 * producing the message into a Dead Letter Queue (DLQ) topic
 */
@AllArgsConstructor
@RegisterForReflection
public class GlobalDLQProductionExceptionHandler implements ProductionExceptionHandler {
    /**
     * Delegate object that has been instantiated through Arc
     */
    @Delegate
    private final GlobalDLQProductionExceptionHandlerDelegate delegate;

    /**
     * No-args constructor used by KafkaStreams.
     * <p>
     * The CDI resolution for injection is performed using a delegate object installation through CDI
     * </p>
     */
    public GlobalDLQProductionExceptionHandler() {
        this(CDI.current().select(GlobalDLQProductionExceptionHandlerDelegate.class).get());
    }
}
