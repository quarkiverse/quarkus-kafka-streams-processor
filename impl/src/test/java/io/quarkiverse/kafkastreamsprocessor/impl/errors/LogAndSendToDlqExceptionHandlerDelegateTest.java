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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler.DeserializationHandlerResponse;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.TaskId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.quarkiverse.kafkastreamsprocessor.impl.metrics.KafkaStreamsProcessorMetrics;
import io.quarkiverse.kafkastreamsprocessor.impl.metrics.MockKafkaStreamsProcessorMetrics;
import io.quarkiverse.kafkastreamsprocessor.spi.properties.DlqConfig;
import io.quarkiverse.kafkastreamsprocessor.spi.properties.KStreamsProcessorConfig;

@ExtendWith(MockitoExtension.class)
class LogAndSendToDlqExceptionHandlerDelegateTest {
    private static final String DLQ_TOPIC = "dql-topic";

    @Mock
    private KStreamsProcessorConfig kStreamsProcessorConfig;

    @Mock
    private DlqConfig dlqConfig;

    @Mock
    private ProcessorContext context;

    @Mock
    DlqProducerService producerService;

    @Mock
    private ConsumerRecord<byte[], byte[]> record;

    @Mock
    private Exception exception;

    private KafkaStreamsProcessorMetrics metrics = new MockKafkaStreamsProcessorMetrics();

    private LogAndSendToDlqExceptionHandlerDelegate handler;

    @BeforeEach
    void setUp() {
        when(kStreamsProcessorConfig.dlq()).thenReturn(dlqConfig);
    }

    @Test
    void shouldNotBlockAndSendToDlqIfPossible() {
        when(dlqConfig.topic()).thenReturn(Optional.of(DLQ_TOPIC));
        when(kStreamsProcessorConfig.errorStrategy()).thenReturn(ErrorHandlingStrategy.DEAD_LETTER_QUEUE);
        when(context.taskId()).thenReturn(new TaskId(0, 0));

        handler = new LogAndSendToDlqExceptionHandlerDelegate(metrics, kStreamsProcessorConfig, producerService);
        handler.configure(Collections.emptyMap());

        DeserializationHandlerResponse response = handler.handle(context, record, exception);
        assertEquals(DeserializationHandlerResponse.CONTINUE, response);

        InOrder inOrder = inOrder(producerService);
        inOrder.verify(producerService).configure(any());
        inOrder.verify(producerService).sendToDlq(
                eq(record),
                eq(exception),
                eq(new TaskId(0, 0)),
                eq(true));

        assertThat(metrics.processorErrorCounter().count(), closeTo(1d, 0.01d));
    }

    @Test
    void shouldOnlyContinueIfDefaultErrorStrategy() {
        when(kStreamsProcessorConfig.errorStrategy()).thenReturn("continue");
        when(dlqConfig.topic()).thenReturn(Optional.of(DLQ_TOPIC));
        when(kStreamsProcessorConfig.dlq()).thenReturn(dlqConfig);
        when(dlqConfig.topic()).thenReturn(Optional.of(DLQ_TOPIC));
        handler = new LogAndSendToDlqExceptionHandlerDelegate(metrics, kStreamsProcessorConfig, producerService);
        handler.configure(Collections.emptyMap());

        DeserializationHandlerResponse response = handler.handle(context, record, exception);
        assertEquals(DeserializationHandlerResponse.CONTINUE, response);

        verify(producerService).configure(any());
        verify(producerService, never()).sendToDlq(any(), any(), any(), any());
        assertThat(metrics.processorErrorCounter().count(), closeTo(1d, 0.01d));
    }

    @Test
    void shouldFailFastIfDlqStrategyWithoutTopic() {
        when(dlqConfig.topic()).thenReturn(Optional.empty());
        when(kStreamsProcessorConfig.dlq()).thenReturn(dlqConfig);
        when(kStreamsProcessorConfig.errorStrategy()).thenReturn(ErrorHandlingStrategy.DEAD_LETTER_QUEUE);
        handler = new LogAndSendToDlqExceptionHandlerDelegate(metrics, kStreamsProcessorConfig, producerService);

        assertThrows(IllegalStateException.class, () -> handler.configure(Collections.emptyMap()));
    }
}
