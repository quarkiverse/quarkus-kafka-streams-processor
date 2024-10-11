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
package io.quarkiverse.kafkastreamsprocessor.impl.decorator.processor;

import jakarta.annotation.Priority;
import jakarta.decorator.Decorator;
import jakarta.decorator.Delegate;
import jakarta.inject.Inject;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;

import io.quarkiverse.kafkastreamsprocessor.api.decorator.processor.ProcessorDecoratorPriorities;
import io.quarkiverse.kafkastreamsprocessor.impl.metrics.KafkaStreamsProcessorMetrics;

/**
 * Decorator to enrich Kafka Streams metrics with a counter of exception raised by {@link Processor#process(Record)}.
 */
@Decorator
@Priority(ProcessorDecoratorPriorities.METRICS)
public class MetricsDecorator<KIn, VIn, KOut, VOut> implements Processor<KIn, VIn, KOut, VOut> {
    /**
     * Injection point for composition.
     */
    @lombok.experimental.Delegate(excludes = Excludes.class)
    private final Processor<KIn, VIn, KOut, VOut> delegate;

    /**
     * Counter of exception raised by {@link Processor#process(Record)}.
     */
    private final KafkaStreamsProcessorMetrics metrics;

    /**
     * Injection constructor.
     *
     * @param delegate
     *        injection point for composition
     * @param metrics
     *        container of all the metrics defined by the framework
     */
    @Inject
    public MetricsDecorator(@Delegate Processor<KIn, VIn, KOut, VOut> delegate,
            KafkaStreamsProcessorMetrics metrics) {
        this.delegate = delegate;
        this.metrics = metrics;
    }

    /**
     * Decorates processing to intercept exceptions in order to count them.
     * <p>
     * Original documentation:
     * </p>
     * {@inheritDoc}
     */
    @Override
    public void process(Record<KIn, VIn> record) {
        try {
            delegate.process(record);
        } catch (Exception e) { // NOSONAR: Catching any error
            metrics.processorErrorCounter().increment();
            throw e;
        }
    }

    private interface Excludes {
        <KIn, VIn> void process(Record<KIn, VIn> record);
    }

}
