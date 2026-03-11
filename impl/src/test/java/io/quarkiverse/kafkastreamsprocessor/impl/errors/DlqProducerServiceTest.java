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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.processor.TaskId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.quarkiverse.kafkastreamsprocessor.impl.KafkaClientSupplierDecorator;
import io.quarkiverse.kafkastreamsprocessor.impl.metrics.KafkaStreamsProcessorMetrics;
import io.quarkiverse.kafkastreamsprocessor.impl.metrics.MockKafkaStreamsProcessorMetrics;
import io.quarkiverse.kafkastreamsprocessor.spi.properties.DlqConfig;
import io.quarkiverse.kafkastreamsprocessor.spi.properties.KStreamsProcessorConfig;

@ExtendWith(MockitoExtension.class)
class DlqProducerServiceTest {

    private static final String DLQ_TOPIC = "dlq-topic";
    private static final String SOURCE_TOPIC = "source-topic";
    private static final int PARTITION = 0;
    private static final long OFFSET = 42L;
    private static final long TIMESTAMP = 1234L;
    private static final byte[] KEY = "key".getBytes();
    private static final byte[] VALUE = "value".getBytes();

    @Mock
    KafkaClientSupplier kafkaClientSupplier;

    @Mock
    DlqMetadataHandler dlqMetadataHandler;

    @Mock
    KStreamsProcessorConfig kStreamsProcessorConfig;

    @Mock
    DlqConfig dlqConfig;

    @Mock
    Producer<byte[], byte[]> dlqProducer;

    @Captor
    ArgumentCaptor<ProducerRecord<byte[], byte[]>> producerRecordCaptor;

    KafkaStreamsProcessorMetrics metrics = new MockKafkaStreamsProcessorMetrics();

    DlqProducerService service;

    @Mock
    ConsumerRecord<byte[], byte[]> consumerRecord;

    @BeforeEach
    void setUp() {
        when(kStreamsProcessorConfig.dlq()).thenReturn(dlqConfig);
        service = new DlqProducerService(kafkaClientSupplier, metrics, dlqMetadataHandler, kStreamsProcessorConfig);
    }

    @Test
    void shouldCreateDlqProducerOnConfigureWhenDlqIsEnabled() {
        when(dlqConfig.topic()).thenReturn(Optional.of(DLQ_TOPIC));
        when(kStreamsProcessorConfig.errorStrategy()).thenReturn(ErrorHandlingStrategy.DEAD_LETTER_QUEUE);
        when(kafkaClientSupplier.getProducer(any())).thenReturn(dlqProducer);

        service.configure(Collections.emptyMap());

        assertThat(service.sendToDlq, is(true));
        verify(kafkaClientSupplier)
                .getProducer(argThat(config -> Boolean.TRUE.equals(config.get(KafkaClientSupplierDecorator.DLQ_PRODUCER))));
    }

    @Test
    void shouldNotCreateDlqProducerOnConfigureWhenDlqIsDisabled() {
        when(dlqConfig.topic()).thenReturn(Optional.of(DLQ_TOPIC));
        when(kStreamsProcessorConfig.errorStrategy()).thenReturn(ErrorHandlingStrategy.CONTINUE);

        service.configure(Collections.emptyMap());

        verify(kafkaClientSupplier, never()).getProducer(any());
        assertThat(service.sendToDlq, is(false));
    }

    @Test
    void shouldThrowWhenDlqStrategyConfiguredWithoutTopic() {
        when(dlqConfig.topic()).thenReturn(Optional.empty());
        when(kStreamsProcessorConfig.errorStrategy()).thenReturn(ErrorHandlingStrategy.DEAD_LETTER_QUEUE);

        assertThrows(IllegalStateException.class, () -> service.configure(Collections.emptyMap()));
    }

    @Test
    void shouldSendRecordToDlqTopicWithCorrectFields() {
        when(dlqConfig.topic()).thenReturn(Optional.of(DLQ_TOPIC));
        when(kStreamsProcessorConfig.errorStrategy()).thenReturn(ErrorHandlingStrategy.DEAD_LETTER_QUEUE);
        when(kafkaClientSupplier.getProducer(any())).thenReturn(dlqProducer);
        RecordHeaders enrichedHeaders = new RecordHeaders();
        when(dlqMetadataHandler.withMetadata(any(), any(), any(), any())).thenReturn(enrichedHeaders);
        when(consumerRecord.topic()).thenReturn(SOURCE_TOPIC);
        when(consumerRecord.partition()).thenReturn(PARTITION);
        when(consumerRecord.offset()).thenReturn(OFFSET);
        when(consumerRecord.timestamp()).thenReturn(TIMESTAMP);
        when(consumerRecord.key()).thenReturn(KEY);
        when(consumerRecord.value()).thenReturn(VALUE);
        when(consumerRecord.headers()).thenReturn(new RecordHeaders());

        //Call configure first to initialize properly the dlq producer
        service.configure(Collections.emptyMap());

        RuntimeException exception = new RuntimeException("processing error");
        service.sendToDlq(consumerRecord, exception, new TaskId(0, PARTITION), false);

        verify(dlqProducer).send(producerRecordCaptor.capture(), any());
        ProducerRecord<byte[], byte[]> sentRecord = producerRecordCaptor.getValue();
        assertThat(sentRecord.topic(), is(DLQ_TOPIC));
        assertThat(sentRecord.key(), is(KEY));
        assertThat(sentRecord.value(), is(VALUE));
        assertThat(sentRecord.timestamp(), is(TIMESTAMP));
        assertThat(sentRecord.headers(), is(enrichedHeaders));
    }

    @Test
    void shouldIncrementDlqSentMetricOnSendToDlq() {
        when(dlqConfig.topic()).thenReturn(Optional.of(DLQ_TOPIC));
        when(kStreamsProcessorConfig.errorStrategy()).thenReturn(ErrorHandlingStrategy.DEAD_LETTER_QUEUE);
        when(kafkaClientSupplier.getProducer(any())).thenReturn(dlqProducer);
        when(dlqMetadataHandler.withMetadata(any(), any(), any(), any())).thenReturn(new RecordHeaders());
        when(consumerRecord.headers()).thenReturn(new RecordHeaders());
        when(consumerRecord.timestamp()).thenReturn(TIMESTAMP);

        //Call configure first to initialize properly the dlq producer
        service.configure(Collections.emptyMap());

        service.sendToDlq(consumerRecord, new RuntimeException("error"), new TaskId(0, PARTITION), false);

        assertThat(metrics.microserviceDlqSentCounter().count(), closeTo(1d, 0.01d));
    }

    @Test
    void shouldEnrichHeadersWithMetadataOnSendToDlq() {
        when(dlqConfig.topic()).thenReturn(Optional.of(DLQ_TOPIC));
        when(kStreamsProcessorConfig.errorStrategy()).thenReturn(ErrorHandlingStrategy.DEAD_LETTER_QUEUE);
        when(kafkaClientSupplier.getProducer(any())).thenReturn(dlqProducer);
        RecordHeaders originalHeaders = new RecordHeaders();
        when(dlqMetadataHandler.withMetadata(any(), any(), any(), any())).thenReturn(new RecordHeaders());
        when(consumerRecord.topic()).thenReturn(SOURCE_TOPIC);
        when(consumerRecord.partition()).thenReturn(PARTITION);
        when(consumerRecord.headers()).thenReturn(originalHeaders);
        when(consumerRecord.timestamp()).thenReturn(TIMESTAMP);

        //Call configure first to initialize properly the dlq producer
        service.configure(Collections.emptyMap());

        RuntimeException exception = new RuntimeException("processing error");
        service.sendToDlq(consumerRecord, exception, new TaskId(0, PARTITION), false);

        verify(dlqMetadataHandler).withMetadata(originalHeaders, SOURCE_TOPIC, PARTITION, exception);
    }
}
