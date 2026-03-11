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

import java.util.Map;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;

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
     * Metrics container for this framework
     */
    private final KafkaStreamsProcessorMetrics metrics;

    /**
     * Class containing the configuration related to kafka streams processor
     */
    private final KStreamsProcessorConfig kStreamsProcessorConfig;

    /**
     * The service able to produce messages to the local DLQ
     */
    private final DlqProducerService dlqDelegate;

    /** True if the dead letter queue strategy is selected and properly configured */
    private boolean sendToDlq;

    /**
     * Injection constructor
     *
     * @param metrics the metrics container of this framework
     * @param kStreamsProcessorConfig The configuration related to kafka processor
     * @param dlqDelegate The service able to produce messages to the local DLQ
     */
    @Inject
    public LogAndSendToDlqExceptionHandlerDelegate(KafkaStreamsProcessorMetrics metrics,
            KStreamsProcessorConfig kStreamsProcessorConfig,
            DlqProducerService dlqDelegate) {
        this.metrics = metrics;
        this.kStreamsProcessorConfig = kStreamsProcessorConfig;
        this.dlqDelegate = dlqDelegate;
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
            dlqDelegate.sendToDlq(record, exception, context.taskId(), true);
        } else {
            // No DLQ to send message to, drop it
            log.error("Exception caught during Deserialization, message dropped; " +
                    "taskId: {}, topic: {}, partition: {}, offset: {}",
                    context.taskId(), record.topic(), record.partition(), record.offset(),
                    exception);
        }
        return DeserializationHandlerResponse.CONTINUE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final Map<String, ?> configs) {
        // Resolve the DLQ strategy once to fail fast in case of misconfiguration
        sendToDlq = ErrorHandlingStrategy.shouldSendToDlq(kStreamsProcessorConfig.errorStrategy(),
                kStreamsProcessorConfig.dlq().topic());
        dlqDelegate.configure(configs);
    }
}
