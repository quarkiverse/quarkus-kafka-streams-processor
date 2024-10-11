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

import java.util.Optional;
import java.util.Set;

import jakarta.annotation.Priority;
import jakarta.decorator.Decorator;
import jakarta.decorator.Delegate;
import jakarta.inject.Inject;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;

import io.quarkiverse.kafkastreamsprocessor.api.decorator.processor.ProcessorDecoratorPriorities;
import io.quarkiverse.kafkastreamsprocessor.impl.TopologyProducer;
import io.quarkiverse.kafkastreamsprocessor.impl.errors.DlqMetadataHandler;
import io.quarkiverse.kafkastreamsprocessor.impl.errors.ErrorHandlingStrategy;
import io.quarkiverse.kafkastreamsprocessor.impl.metrics.KafkaStreamsProcessorMetrics;
import io.quarkiverse.kafkastreamsprocessor.spi.SinkToTopicMappingBuilder;
import io.quarkiverse.kafkastreamsprocessor.spi.properties.KStreamsProcessorConfig;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

/**
 * Forwards poisonous messages to the dead-letter sink.
 * <p>
 * Uses a dead-letter sink from the topology, rather than a raw producer, to benefit from the same KStreams guarantees
 * (at least once / exactly once).
 */
@Decorator
@Priority(ProcessorDecoratorPriorities.DLQ)
public class DlqDecorator<KIn, VIn, KOut, VOut> implements Processor<KIn, VIn, KOut, VOut> {
    /**
     * Inject point for composition
     */
    @lombok.experimental.Delegate(excludes = Excludes.class)
    private final Processor<KIn, VIn, KOut, VOut> delegate;

    /**
     * A set of sink names that are involved in the business logic.
     * <p>
     * Typically the dead-letter queue is excluded here.
     * </p>
     */
    private final Set<String> functionalSinks;

    /**
     * Tool to enrich a message metadata before its storage in the dead letter queue
     */
    private final DlqMetadataHandler dlqMetadataHandler;

    /**
     * container of all metrics of the framework
     */
    private final KafkaStreamsProcessorMetrics metrics;

    /**
     * Whether the dead-letter queue mechanism is activated for this microservice
     */
    private final boolean activated;

    /**
     * Keeping a reference to the ProcessorContext to be able to use it in the {@link Processor#process(Record)} method
     * whilst not implementing the little too narrowing {@link ContextualProcessor}.
     */
    private ProcessorContext<KOut, VOut> context;

    DlqDecorator(Processor<KIn, VIn, KOut, VOut> delegate, Set<String> functionalSinks, DlqMetadataHandler dlqMetadataHandler,
            KafkaStreamsProcessorMetrics metrics, boolean activated) {
        this.delegate = delegate;
        this.functionalSinks = functionalSinks;
        this.dlqMetadataHandler = dlqMetadataHandler;
        this.metrics = metrics;
        this.activated = activated;
    }

    /**
     * Injection constructor
     *
     * @param delegate
     *        inject point for composition, as mandated by the {@link Decorator} mechanism from CDI
     * @param sinkToTopicMappingBuilder
     *        utility to get access to the mapping between sinks and Kafka topics
     * @param dlqMetadataHandler
     *        the enricher of metadata before sending message to the dead letter queue
     * @param metrics
     *        container of all metrics of the framework
     * @param kStreamsProcessorConfig
     *        It contains the configuration for the error strategy configuration property value (default
     *        {@link ErrorHandlingStrategy#CONTINUE})
     *        and the configuration Kafka topic to use for dead letter queue (optional)
     */
    @Inject
    public DlqDecorator(@Delegate Processor<KIn, VIn, KOut, VOut> delegate,
            SinkToTopicMappingBuilder sinkToTopicMappingBuilder, DlqMetadataHandler dlqMetadataHandler,
            KafkaStreamsProcessorMetrics metrics,
            KStreamsProcessorConfig kStreamsProcessorConfig) { // NOSONAR Optional with microprofile-config
        this(delegate, sinkToTopicMappingBuilder.sinkToTopicMapping().keySet(), dlqMetadataHandler, metrics,
                ErrorHandlingStrategy.shouldSendToDlq(kStreamsProcessorConfig.errorStrategy(),
                        kStreamsProcessorConfig.dlq().topic()));
    }

    /**
     * Decorates initialization to inject a decorated {@link ProcessorContext} that does not systematically forward the
     * message to the dead letter queue. The counter metric is also initialized.
     * <p>
     * Original documentation:
     * </p>
     * {@inheritDoc}
     */
    @Override
    public void init(final ProcessorContext<KOut, VOut> context) {
        if (activated) {
            this.context = new DlqProcessorContextDecorator<>((InternalProcessorContext<KOut, VOut>) context, functionalSinks);
            delegate.init(this.context);
        } else {
            delegate.init(context);
        }
    }

    /**
     * Decorates processing to catch {@link KafkaException} (that are typically raised when something happened during the
     * production of the message in the outgoing topic(s)) and in that case write the message to the dead letter queue and
     * incrementing the counter.
     * <p>
     * Original documentation:
     * </p>
     * {@inheritDoc}
     */
    @Override
    public void process(Record<KIn, VIn> record) {
        if (activated) {
            try {
                delegate.process(record);
            } catch (KafkaException e) {
                // Do not forward to DLQ
                throw e;
            } catch (RuntimeException e) { // NOSONAR
                Optional<RecordMetadata> recordMetadata = context.recordMetadata();
                if (recordMetadata.isPresent()) {
                    dlqMetadataHandler.addMetadata(record.headers(), recordMetadata.get().topic(),
                            recordMetadata.get().partition(), e);
                    context.forward((Record<KOut, VOut>) record, TopologyProducer.DLQ_SINK_NAME);
                    // Re-throw so the exception gets logged
                    metrics.microserviceDlqSentCounter().increment();
                    throw e;
                }
            }
        } else {
            delegate.process(record);
        }
    }

    @RequiredArgsConstructor(access = AccessLevel.MODULE)
    static final class DlqProcessorContextDecorator<KOut, VOut> implements InternalProcessorContext<KOut, VOut> {

        @lombok.experimental.Delegate(excludes = ExcludeMethods.class)
        private final InternalProcessorContext<KOut, VOut> delegate;

        private final Set<String> functionalSinks;

        @Override
        public <K extends KOut, V extends VOut> void forward(Record<K, V> record) {
            functionalSinks.forEach(functionalSink -> delegate.forward(record, functionalSink));
        }

        @Override
        public <K, V> void forward(K key, V value) {
            functionalSinks.forEach(functionalSink -> delegate.forward(key, value, To.child(functionalSink)));
        }

        public <K extends KOut, V extends VOut> void forward(FixedKeyRecord<K, V> record) {
            functionalSinks.forEach(functionalSink -> delegate.forward(record, functionalSink));
        }

        private interface ExcludeMethods {
            <K, V> void forward(FixedKeyRecord<K, V> record);

            <K, V> void forward(final K key, final V value);

            <K, V> void forward(Record<K, V> record);
        }
    }

    private interface Excludes {
        <KOut, VOut> void init(ProcessorContext<KOut, VOut> context);

        <KIn, VIn> void process(Record<KIn, VIn> record);
    }
}
