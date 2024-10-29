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

import java.util.HashMap;
import java.util.Map;

import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;

import io.quarkiverse.kafkastreamsprocessor.api.decorator.producer.ProducerOnSendInterceptor;
import io.quarkiverse.kafkastreamsprocessor.impl.KafkaClientSupplierDecorator;
import io.quarkiverse.kafkastreamsprocessor.impl.metrics.KafkaStreamsProcessorMetrics;
import io.quarkiverse.kafkastreamsprocessor.spi.properties.KStreamsProcessorConfig;
import io.quarkus.arc.Unremovable;
import lombok.extern.slf4j.Slf4j;

/**
 * <p>
 * Alternate non-blocking {@link DeserializationExceptionHandler} where the poisonous message is forwarded to the dead
 * letter queue topic instead of being dropped like the {@link LogAndContinueExceptionHandler}.
 * </p>
 * If no DLQ topic is found, drop the message.
 */
@Slf4j
@ApplicationScoped
@Unremovable
public class LogAndSendToDlqExceptionHandlerDelegate implements DeserializationExceptionHandler {
    /**
     * Tool object that enriches the metadata of messages before sending them to the microservice's specific DLQ.
     */
    private final DlqMetadataHandler dlqMetadataHandler;

    /**
     * Metrics container for this framework
     */
    private final KafkaStreamsProcessorMetrics metrics;

    /**
     * Kafka producer supplier for this framework
     */
    private final KafkaClientSupplier clientSupplier;

    /**
     * The class containing all the configuration related to kafka stream processor
     */
    private final KStreamsProcessorConfig kStreamsProcessorConfig;

    private final ErrorHandlingStrategy errorHandlingStrategy;

    /** True if the dead letter queue strategy is selected and properly configured */
    boolean sendToDlq;

    /** Producer for the dlq topic */
    Producer<byte[], byte[]> dlqProducer;

    /**
     * Injection constructor
     *
     * @param kafkaClientSupplier a supplier of {@link org.apache.kafka.clients.producer.KafkaProducer}
     * @param metrics the metrics container of this framework
     * @param dlqMetadataHandler tool to enrich message metadata before sending them to the microservice's DLQ
     *        the configuration error strategy for the application. See { @link {@link ErrorHandlingStrategy}
     * @param kStreamsProcessorConfig The configuration related to kafka processor
     */
    @Inject
    public LogAndSendToDlqExceptionHandlerDelegate(KafkaClientSupplier kafkaClientSupplier,
            KafkaStreamsProcessorMetrics metrics,
            DlqMetadataHandler dlqMetadataHandler,
            KStreamsProcessorConfig kStreamsProcessorConfig,
            ErrorHandlingStrategy errorHandlingStrategy) {
        this.clientSupplier = kafkaClientSupplier;
        this.metrics = metrics;
        this.dlqMetadataHandler = dlqMetadataHandler;
        this.kStreamsProcessorConfig = kStreamsProcessorConfig;
        this.errorHandlingStrategy = errorHandlingStrategy;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DeserializationHandlerResponse handle(final ProcessorContext context,
            final ConsumerRecord<byte[], byte[]> record,
            final Exception exception) {
        metrics.processorErrorCounter().increment();
        if (sendToDlq) {
            sendToDlq(context, record, exception);
        } else {
            // No DLQ to send message to, drop it
            log.error("Exception caught during Deserialization, message dropped; " +
                    "taskId: {}, topic: {}, partition: {}, offset: {}",
                    context.taskId(), record.topic(), record.partition(), record.offset(),
                    exception);
        }
        return DeserializationHandlerResponse.CONTINUE;
    }

    private void sendToDlq(final ProcessorContext context, final ConsumerRecord<byte[], byte[]> record,
            final Exception exception) {
        // Send to dead letter queue
        metrics.microserviceDlqSentCounter().increment();
        log.error("Exception caught during Deserialization, sending to the dead letter queue topic; " +
                "taskId: {}, topic: {}, partition: {}, offset: {}",
                context.taskId(), record.topic(), record.partition(), record.offset(),
                exception);

        // We cannot use context.forward here: we are given a fake context without source information
        // https://issues.apache.org/jira/browse/KAFKA-9566
        dlqProducer.send(new ProducerRecord<>(kStreamsProcessorConfig.dlq().topic().get(), null, record.timestamp(),
                record.key(), record.value(),
                dlqMetadataHandler.withMetadata(record.headers(), record.topic(), record.partition(), exception)));
    }

    /**
     * If DLQ is active, it initializes a {@link Producer} for the DLQ with the
     * {@link KafkaClientSupplierDecorator#DLQ_PRODUCER} flag
     * so it is only decorated with {@link ProducerOnSendInterceptor} that have
     * {@link ProducerOnSendInterceptor#skipForDLQ()} returning <code>false</code>.
     * <p>
     * <b>Original documentation:</b>
     * <p>
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> configs) {
        // Resolve the DLQ strategy once to fail fast in case of misconfiguration
        sendToDlq = errorHandlingStrategy.shouldSendToDlq();
        if (sendToDlq) {
            Map<String, Object> dlqConfigMap = new HashMap<>(configs);
            dlqConfigMap.put(KafkaClientSupplierDecorator.DLQ_PRODUCER, true);
            dlqProducer = new LogCallbackExceptionProducerDecorator(clientSupplier.getProducer(dlqConfigMap));
        }
    }

    /**
     * Properly close the producer before Kubernetes forcefully destroys everything
     */
    @PreDestroy
    public void close() {
        if (dlqProducer != null) {
            dlqProducer.close(GlobalDLQProductionExceptionHandlerDelegate.GRACEFUL_PERIOD);
        }
    }
}
