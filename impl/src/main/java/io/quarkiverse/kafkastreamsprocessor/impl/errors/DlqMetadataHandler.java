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

import java.nio.charset.StandardCharsets;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

import io.quarkiverse.kafkastreamsprocessor.impl.protocol.KafkaStreamsProcessorHeaders;

/**
 * Add headers based on smallrye-reactive-messaging-kafka convention. See
 * https://smallrye.io/smallrye-reactive-messaging/3.21.0/kafka/receiving-kafka-records/#failure-management
 */
@ApplicationScoped
public class DlqMetadataHandler {

    /**
     * @see #addMetadata(Headers, String, Integer, Throwable) when the headers are writable, for instance within
     *      {@link org.apache.kafka.streams.processor.ProcessorContext}
     */
    public Headers withMetadata(Headers headers, String topic, Integer partition, Throwable error) {
        Headers headersWithDlq = new RecordHeaders(headers.toArray());
        addMetadata(headersWithDlq, topic, partition, error);
        return headersWithDlq;
    }

    /**
     * @see #withMetadata(Headers, String, Integer, Throwable) when the headers are read-only, for instance with
     *      {@link org.apache.kafka.clients.producer.ProducerRecord} and
     *      {@link org.apache.kafka.clients.consumer.ConsumerRecord}
     */
    public void addMetadata(Headers headers, String topic, Integer partition, Throwable error) {
        if (error.getMessage() != null) {
            headers.add(KafkaStreamsProcessorHeaders.DLQ_REASON, error.getMessage().getBytes(StandardCharsets.UTF_8));
        }
        if (error.getCause() != null) {
            headers.add(KafkaStreamsProcessorHeaders.DLQ_CAUSE,
                    error.getCause().getClass().getName().getBytes(StandardCharsets.UTF_8));
        }
        headers.add(KafkaStreamsProcessorHeaders.DLQ_TOPIC, topic.getBytes(StandardCharsets.UTF_8));
        if (partition != null) {
            headers.add(KafkaStreamsProcessorHeaders.DLQ_PARTITION,
                    Integer.toString(partition).getBytes(StandardCharsets.UTF_8));
        }
    }
}
