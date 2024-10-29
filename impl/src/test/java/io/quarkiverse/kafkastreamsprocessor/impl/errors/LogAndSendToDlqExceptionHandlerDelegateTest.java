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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler.DeserializationHandlerResponse;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.quarkiverse.kafkastreamsprocessor.impl.metrics.KafkaStreamsProcessorMetrics;
import io.quarkiverse.kafkastreamsprocessor.impl.metrics.MockKafkaStreamsProcessorMetrics;
import io.quarkiverse.kafkastreamsprocessor.spi.properties.DlqConfig;
import io.quarkiverse.kafkastreamsprocessor.spi.properties.KStreamsProcessorConfig;

@ExtendWith(MockitoExtension.class)
class LogAndSendToDlqExceptionHandlerDelegateTest {
    private static final byte[] RECORD_KEY_BYTES = "KEY".getBytes(StandardCharsets.UTF_8);
    private static final byte[] RECORD_VALUE_BYTES = "VALUE".getBytes(StandardCharsets.UTF_8);
    private static final long RECORD_TIMESTAMP = 1234L;
    private static final String DLQ_TOPIC = "dql-topic";
    private static final String INPUT_TOPIC = "input-topic";
    private static final int PARTITION = 0;

    @Mock
    private KStreamsProcessorConfig kStreamsProcessorConfig;

    @Mock
    private DlqConfig dlqConfig;

    @Mock
    private ErrorHandlingStrategy errorHandlingStrategy;

    @Mock
    private ProcessorContext context;

    @Mock
    private Producer<byte[], byte[]> dlqProducerMock;

    @Mock
    KafkaClientSupplier kafkaClientSupplier;

    @Mock
    private ConsumerRecord<byte[], byte[]> record;

    @Mock
    private Exception exception;

    @Mock
    private DlqMetadataHandler metadataHandler;

    @Mock
    private Headers headers;

    private KafkaStreamsProcessorMetrics metrics = new MockKafkaStreamsProcessorMetrics();

    private LogAndSendToDlqExceptionHandlerDelegate handler;

    @BeforeEach
    void setUp() {
      when(kStreamsProcessorConfig.dlq()).thenReturn(dlqConfig);
    }

  @Test
  void shouldNotBlockAndSendToDlqIfPossible() {
    when(record.key()).thenReturn(RECORD_KEY_BYTES);
    when(record.value()).thenReturn(RECORD_VALUE_BYTES);
    when(record.timestamp()).thenReturn(RECORD_TIMESTAMP);
    when(record.topic()).thenReturn(INPUT_TOPIC);
    when(record.partition()).thenReturn(PARTITION);
    when(record.headers()).thenReturn(headers);
    when(dlqConfig.topic()).thenReturn(Optional.of(DLQ_TOPIC));
    when(errorHandlingStrategy.shouldSendToDlq()).thenReturn(true);
    RecordHeaders headersWithMetadata = new RecordHeaders();
    when(metadataHandler.withMetadata(any(Headers.class), anyString(), anyInt(), any(Exception.class)))
        .thenReturn(headersWithMetadata);
    when(kafkaClientSupplier.getProducer(any())).thenReturn(dlqProducerMock);

    handler = new LogAndSendToDlqExceptionHandlerDelegate(kafkaClientSupplier, metrics, metadataHandler,
      kStreamsProcessorConfig, errorHandlingStrategy);
    handler.configure(Collections.emptyMap());

    DeserializationHandlerResponse response = handler.handle(context, record, exception);
    assertEquals(DeserializationHandlerResponse.CONTINUE, response);

    verify(dlqProducerMock)
        .send(eq(new ProducerRecord<>(DLQ_TOPIC, null, RECORD_TIMESTAMP, RECORD_KEY_BYTES, RECORD_VALUE_BYTES,
            headersWithMetadata)), any(Callback.class));
    verify(metadataHandler).withMetadata(eq(headers), eq(INPUT_TOPIC), eq(PARTITION), eq(exception));
    assertThat(metrics.processorErrorCounter().count(), closeTo(1d, 0.01d));
  }

    @Test
    void shouldOnlyContinueIfDefaultErrorStrategy() {
      when(errorHandlingStrategy.shouldSendToDlq()).thenReturn(false);
        when(dlqConfig.topic()).thenReturn(Optional.of(DLQ_TOPIC));
        when(kStreamsProcessorConfig.dlq()).thenReturn(dlqConfig);
        when(dlqConfig.topic()).thenReturn(Optional.of(DLQ_TOPIC));
        handler = new LogAndSendToDlqExceptionHandlerDelegate(kafkaClientSupplier, metrics, metadataHandler,
          kStreamsProcessorConfig, errorHandlingStrategy);
        handler.configure(Collections.emptyMap());

        DeserializationHandlerResponse response = handler.handle(context, record, exception);
        assertEquals(DeserializationHandlerResponse.CONTINUE, response);

        verifyNoInteractions(dlqProducerMock);
        assertThat(metrics.processorErrorCounter().count(), closeTo(1d, 0.01d));
    }

}
