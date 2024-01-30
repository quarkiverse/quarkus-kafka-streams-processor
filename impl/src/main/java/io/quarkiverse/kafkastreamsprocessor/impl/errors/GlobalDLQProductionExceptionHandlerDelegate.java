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

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;

import io.quarkiverse.kafkastreamsprocessor.impl.KafkaClientSupplierDecorator;
import io.quarkiverse.kafkastreamsprocessor.impl.metrics.KafkaStreamsProcessorMetrics;
import io.quarkiverse.kafkastreamsprocessor.spi.properties.KStreamsProcessorConfig;
import io.quarkus.arc.Unremovable;
import lombok.extern.slf4j.Slf4j;

/**
 * This class is responsible of handling exception thrown when KafkaStreams cannot produce in any downstream topics
 * <p>
 * The strategy is to continue streaming messages, but the message that is the cause of the production error is piped
 * into a global DLQ topic
 * </p>
 */
@ApplicationScoped
@Unremovable
@Slf4j
class GlobalDLQProductionExceptionHandlerDelegate implements ProductionExceptionHandler {

    /**
     * Implicit graceful period based on Kubernetes default graceful period which is 30 seconds
     */
    protected static final Duration GRACEFUL_PERIOD = Duration.ofSeconds(29L);

    /**
     * client supplier to create a message producer if global DLQ is configured
     */
    private final KafkaClientSupplier clientSupplier;

    /**
     * tool that enriches the metadata of messages with some context before sending them to the global DLQ
     */
    private final DlqMetadataHandler dlqMetadataHandler;

    /**
     * the metrics container of this framework
     */
    private final KafkaStreamsProcessorMetrics metrics;

    /**
     * Configuration class for Kafka Producer
     */
    private final KStreamsProcessorConfig kStreamsProcessorConfig;

    /**
     * Kafka message producer responsible to send messages to the global DLQ
     */
    private Producer<byte[], byte[]> kafkaProducer;

    /**
     * Injection constructor
     *
     * @param kafkaClientSupplier client supplier to create a message producer if global DLQ is configured
     * @param dlqMetadataHandler tool that enriches the metadata of messages with some context before sending them to the global
     *        DLQ
     * @param metrics the metrics container of this framework
     * @param kStreamsProcessorConfig
     */
    @Inject
    public GlobalDLQProductionExceptionHandlerDelegate(KafkaClientSupplier kafkaClientSupplier,
            DlqMetadataHandler dlqMetadataHandler, KafkaStreamsProcessorMetrics metrics,
            KStreamsProcessorConfig kStreamsProcessorConfig) {
        this.clientSupplier = kafkaClientSupplier;
        this.dlqMetadataHandler = dlqMetadataHandler;
        this.metrics = metrics;
        this.kStreamsProcessorConfig = kStreamsProcessorConfig;
    }

    /**
     * If configured any message caught by this {@link ProductionExceptionHandler} is sent to the global DLQ as a last
     * resort.
     * <p>
     * If it is not configured, a warning is logged and the processor will be given the next message in the partition.
     * </p>
     */
    @Override
    public ProductionExceptionHandlerResponse handle(ProducerRecord<byte[], byte[]> record, Exception exception) {
        if (kafkaProducer != null && kStreamsProcessorConfig.globalDlq().topic().isPresent()) {
            sendToGlobalDlq(record, exception);
        } else {
            log.warn("Exception caught during production but no GlobalDLQ is configured, incoming message " +
                    "will be consumed",
                    exception);
        }

        return ProductionExceptionHandlerResponse.CONTINUE;
    }

    private void sendToGlobalDlq(ProducerRecord<byte[], byte[]> record, Exception exception) {
        metrics.globalDlqSentCounter().increment();
        log.error("Exception caught during production, sending to the dead letter queue topic; " +
                "topic: {}, partition: {}",
                record.topic(), record.partition(), exception);
        ProducerRecord<byte[], byte[]> dlqRecord = new ProducerRecord<>(
                kStreamsProcessorConfig.globalDlq().topic().get(),
                null, record.key(),
                record.value(),
                dlqMetadataHandler.withMetadata(record.headers(), record.topic(), record.partition(), exception));
        kafkaProducer.send(dlqRecord);
    }

    /**
     * Peculiar configure implementation that will instantiate a KafkaProducer based on the configuration, before
     * decorating it with {@link LogCallbackExceptionProducerDecorator} that blocks the exception propagation on callback
     * that would prevent the global DLQ to play its role of last resort.
     *
     * @param config
     *        the default producer configuration
     */
    @Override
    public void configure(Map<String, ?> config) {
        Map<String, Object> producerConfig = (Map<String, Object>) config;

        if (kStreamsProcessorConfig.globalDlq().topic().isPresent()) {
            Map<String, Object> dqlProducerConfig = new HashMap<>(producerConfig);
            dqlProducerConfig.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG,
                    kStreamsProcessorConfig.globalDlq().maxMessageSize());
            dqlProducerConfig.put(KafkaClientSupplierDecorator.DLQ_PRODUCER, true);
            kafkaProducer = new LogCallbackExceptionProducerDecorator(clientSupplier.getProducer(dqlProducerConfig));
        }
    }

    /**
     * Close callback implementation to impose a graceful period on closing the producer to the global DLQ of 29 seconds
     * in regards to the 30 seconds of the Kubernetes default graceful period.
     */
    @PreDestroy
    public void close() {
        if (kafkaProducer != null) {
            kafkaProducer.close(GRACEFUL_PERIOD);
        }
    }
}
