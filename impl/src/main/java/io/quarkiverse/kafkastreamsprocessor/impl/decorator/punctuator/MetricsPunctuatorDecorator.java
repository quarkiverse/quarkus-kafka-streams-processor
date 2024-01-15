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
package io.quarkiverse.kafkastreamsprocessor.impl.decorator.punctuator;

import jakarta.annotation.Priority;
import jakarta.decorator.Decorator;
import jakarta.decorator.Delegate;
import jakarta.inject.Inject;

import org.apache.kafka.streams.processor.Punctuator;

import io.quarkiverse.kafkastreamsprocessor.api.decorator.punctuator.DecoratedPunctuator;
import io.quarkiverse.kafkastreamsprocessor.api.decorator.punctuator.PunctuatorDecoratorPriorities;
import io.quarkiverse.kafkastreamsprocessor.impl.metrics.KafkaStreamsProcessorMetrics;
import lombok.extern.slf4j.Slf4j;

/**
 * Punctuator decorator that prevents Exception propagation of {@link Punctuator#punctuate(long)}.
 * <p>
 * Indeed otherwise, if a {@link Punctuator} raises any exception, the whole microservice crashes.
 * </p>
 * <p>
 * For observability, the exceptions are still logged and counted in a metric.
 * </p>
 */
@Slf4j
@Decorator
@Priority(PunctuatorDecoratorPriorities.METRICS)
public class MetricsPunctuatorDecorator implements DecoratedPunctuator {
    /**
     * Injection point for composition
     */
    @lombok.experimental.Delegate(excludes = Excludes.class)
    private final DecoratedPunctuator delegate;

    private final KafkaStreamsProcessorMetrics metrics;

    @Inject
    public MetricsPunctuatorDecorator(@Delegate DecoratedPunctuator delegate, KafkaStreamsProcessorMetrics metrics) {
        this.delegate = delegate;
        this.metrics = metrics;
    }

    @Override
    public void punctuate(long timestamp) {
        try {
            delegate.punctuate(timestamp);
        } catch (Exception e) { // NOSONAR: Catching any error
            // Swallowing the exception to prevent Kafka Streams to shutdown the thread and then the microservice
            log.error("Exception raised during punctuate", e);
            metrics.punctuatorErrorCounter().increment();
        }
    }

    private interface Excludes {
        void punctuate(long timestamp);
    }
}
