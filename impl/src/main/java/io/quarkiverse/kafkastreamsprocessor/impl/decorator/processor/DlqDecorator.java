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

import static org.apache.kafka.common.record.TimestampType.CREATE_TIME;

import java.util.Optional;
import java.util.Set;

import jakarta.annotation.Priority;
import jakarta.decorator.Delegate;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;

import io.cloudevents.kafka.CloudEventSerializer;
import io.quarkiverse.kafkastreamsprocessor.api.configuration.Configuration;
import io.quarkiverse.kafkastreamsprocessor.api.decorator.processor.AbstractProcessorDecorator;
import io.quarkiverse.kafkastreamsprocessor.api.decorator.processor.ProcessorDecoratorPriorities;
import io.quarkiverse.kafkastreamsprocessor.impl.errors.DlqProducerService;
import io.quarkiverse.kafkastreamsprocessor.impl.errors.ErrorHandlingStrategy;
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
//@Decorator
@Priority(ProcessorDecoratorPriorities.DLQ)
@Dependent
public class DlqDecorator extends AbstractProcessorDecorator {

    /**
     * A set of sink names that are involved in the business logic.
     * <p>
     * Typically the dead-letter queue is excluded here.
     * </p>
     */
    private final Set<String> functionalSinks;

    /**
     * Whether the dead-letter queue mechanism is activated for this microservice
     */
    private final boolean activated;

    /**
     * Delegate that handles DLQ message production
     */
    private final DlqProducerService dlqDelegate;

    /**
     * The configuration of the Kafka Streams processor
     */
    private final Configuration configuration;

    /**
     * Class containing the configuration related to kafka streams processor
     */
    private final KStreamsProcessorConfig kStreamsProcessorConfig;

    /**
     * Keeping a reference to the ProcessorContext to be able to use it in the {@link Processor#process(Record)} method
     * whilst not implementing the little too narrowing {@link ContextualProcessor}.
     */
    private ProcessorContext context;

    DlqDecorator(Set<String> functionalSinks, boolean activated,
            DlqProducerService dlqDelegate,
            Configuration configuration,
            KStreamsProcessorConfig kStreamsProcessorConfig) {
        this.functionalSinks = functionalSinks;
        this.activated = activated;
        this.dlqDelegate = dlqDelegate;
        this.configuration = configuration;
        this.kStreamsProcessorConfig = kStreamsProcessorConfig;
    }

    /**
     * Injection constructor
     *
     * @param sinkToTopicMappingBuilder
     *        utility to get access to the mapping between sinks and Kafka topics
     * @param kStreamsProcessorConfig
     *        It contains the configuration for the error strategy configuration property value (default
     *        {@link ErrorHandlingStrategy#CONTINUE})
     *        and the configuration Kafka topic to use for dead letter queue (optional)
     * @param dlqDelegate
     *        delegate that handles DLQ production with properly configured producer
     * @param configuration
     *        the configuration of the kafka streams processor
     */
    @Inject
    public DlqDecorator(
            SinkToTopicMappingBuilder sinkToTopicMappingBuilder,
            KStreamsProcessorConfig kStreamsProcessorConfig,
            DlqProducerService dlqDelegate,
            Configuration configuration) { // NOSONAR Optional with microprofile-config
        this(sinkToTopicMappingBuilder.sinkToTopicMapping().keySet(),
                ErrorHandlingStrategy.shouldSendToDlq(kStreamsProcessorConfig.errorStrategy(),
                        kStreamsProcessorConfig.dlq().topic()),
                dlqDelegate, configuration, kStreamsProcessorConfig);
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
    public void init(final ProcessorContext context) {
        if (activated) {
            this.context = new DlqProcessorContextDecorator<>((InternalProcessorContext) context, functionalSinks);
            getDelegate().init(this.context);
        } else {
            getDelegate().init(context);
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
    public void process(Record record) {
        if (activated) {
            try {
                getDelegate().process(record);
            } catch (KafkaException e) {
                // Do not forward to DLQ
                throw e;
            } catch (RuntimeException e) { // NOSONAR
                Optional<RecordMetadata> recordMetadata = context.recordMetadata();
                if (recordMetadata.isPresent()) {
                    Serializer<Object> keySerializer = (Serializer<Object>) configuration.getSourceKeySerde().serializer();
                    Serializer<Object> valueSerializer = (Serializer<Object>) getSourceValueSerializer(configuration,
                            kStreamsProcessorConfig);
                    byte[] serializedKey = keySerializer.serialize(recordMetadata.get().topic(), record.headers(),
                            record.key());
                    byte[] serializedValue = valueSerializer.serialize(recordMetadata.get().topic(), record.headers(),
                            record.value());

                    ConsumerRecord<byte[], byte[]> consumerRecord = new ConsumerRecord<>(
                            recordMetadata.get().topic(),
                            recordMetadata.get().partition(),
                            recordMetadata.get().offset(),
                            record.timestamp(),
                            CREATE_TIME,
                            serializedKey != null ? serializedKey.length : 0,
                            serializedValue != null ? serializedValue.length : 0,
                            serializedKey,
                            serializedValue,
                            record.headers(),
                            Optional.empty());

                    dlqDelegate.sendToDlq(consumerRecord, e, context.taskId(), false);

                    // throw exception to fail OpenTelemetry span
                    throw e;
                }
            }
        } else {
            getDelegate().process(record);
        }
    }

    Serializer<?> getSourceValueSerializer(Configuration configuration,
            KStreamsProcessorConfig kStreamsProcessorConfig) {
        // defaulting to CloudEventSerializer if kafkastreamsprocessor.input.is-cloud-event is true
        if (kStreamsProcessorConfig.input().isCloudEvent()) {
            CloudEventSerializer serializer = new CloudEventSerializer();
            serializer.configure(kStreamsProcessorConfig.dlq().cloudEventSerializerConfig(), false);
            return serializer;
        }
        return configuration.getSourceValueSerde().serializer();
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
