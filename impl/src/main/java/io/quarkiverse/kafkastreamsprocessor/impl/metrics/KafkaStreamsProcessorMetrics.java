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
package io.quarkiverse.kafkastreamsprocessor.impl.metrics;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.apache.kafka.streams.processor.Punctuator;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.Getter;
import lombok.experimental.Accessors;

/**
 * Container for constants linked to names of metrics this implementation instruments.
 */
@Getter
@Accessors(fluent = true)
@ApplicationScoped
public class KafkaStreamsProcessorMetrics {
    /**
     * Processing error metrics, also manipulated in case of deserialization error
     */
    private final Counter processorErrorCounter;

    /**
     * Counter of messages sent to the microservice specific Dead Letter Queue (short DLQ)
     */
    private final Counter microserviceDlqSentCounter;

    /**
     * Counter of messages sent to a dead letter queue global to the entire application (cross microservice, i.e.
     * eventually containing messages of different types)
     */
    private final Counter globalDlqSentCounter;

    /**
     * Counter of exceptions raised and caught by all the {@link Punctuator} configured in the application
     */
    private final Counter punctuatorErrorCounter;

    @Inject
    public KafkaStreamsProcessorMetrics(MeterRegistry registry) {
        processorErrorCounter = Counter.builder("kafkastreamsprocessor.processor.errors")
                .description("Total number of errors encountered during Kafka Streams message processing")
                .register(registry);
        microserviceDlqSentCounter = Counter.builder("kafkastreamsprocessor.dlq.sent")
                .description("Total number of messages sent to DLQ")
                .register(registry);
        globalDlqSentCounter = Counter.builder("kafkastreamsprocessor.global.dlq.sent")
                .description("Counts messages sent to a dead letter queue global to the entire application " +
                        "(cross microservice, i.e. eventually containing messages of different types)")
                .register(registry);
        punctuatorErrorCounter = Counter.builder("kafkastreamsprocessor.punctuation.errors")
                .description("Total number of exception caught and recorded of all the configured Punctuator implementations")
                .register(registry);
    }

}
