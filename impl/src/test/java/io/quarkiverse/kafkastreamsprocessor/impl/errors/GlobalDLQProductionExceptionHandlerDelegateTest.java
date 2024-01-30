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

import static io.quarkiverse.kafkastreamsprocessor.impl.errors.GlobalDLQProductionExceptionHandlerDelegate.GRACEFUL_PERIOD;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Optional;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.errors.ProductionExceptionHandler.ProductionExceptionHandlerResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.quarkiverse.kafkastreamsprocessor.impl.TestException;
import io.quarkiverse.kafkastreamsprocessor.impl.metrics.MockKafkaStreamsProcessorMetrics;
import io.quarkiverse.kafkastreamsprocessor.spi.properties.GlobalDlqConfig;
import io.quarkiverse.kafkastreamsprocessor.spi.properties.KStreamsProcessorConfig;

@ExtendWith(MockitoExtension.class)
class GlobalDLQProductionExceptionHandlerDelegateTest {

    public static final String TOPIC = "downstream";
    public static final String GLOBAL_DLQ_NAME = "globalDlq";
    public static final int MAX_SIZE_DLQ = 10;
    public static final int PARTITION = 0;

    GlobalDLQProductionExceptionHandlerDelegate globalDLQExceptionHandlerDelegate;

    MockKafkaStreamsProcessorMetrics metrics = new MockKafkaStreamsProcessorMetrics();

    @Mock
    KStreamsProcessorConfig kStreamsProcessorConfig;

    @Mock
    GlobalDlqConfig globalDlqConfig;

    @Mock
    KafkaClientSupplier kafkaClientSupplier;

    @Mock
    DlqMetadataHandler dlqMetadataHandler;

    @Mock
    KafkaProducer<byte[], byte[]> producer;

    @Captor
    ArgumentCaptor<ProducerRecord<byte[], byte[]>> producedRecord;

    @BeforeEach
    public void setup() {
        when(kafkaClientSupplier.getProducer(any())).thenReturn(producer);
        when(kStreamsProcessorConfig.globalDlq()).thenReturn(globalDlqConfig);
        when(globalDlqConfig.topic()).thenReturn(Optional.of(GLOBAL_DLQ_NAME));
        when(globalDlqConfig.maxMessageSize()).thenReturn(MAX_SIZE_DLQ);
        globalDLQExceptionHandlerDelegate = new GlobalDLQProductionExceptionHandlerDelegate(kafkaClientSupplier,
                dlqMetadataHandler, metrics, kStreamsProcessorConfig);
        globalDLQExceptionHandlerDelegate.configure(Collections.emptyMap());
    }

    @Test
    public void producerShouldBeClosed() {
        globalDLQExceptionHandlerDelegate.close();
        verify(producer).close(eq(GRACEFUL_PERIOD));
    }

    @Test
    public void shouldPublishToDlqInCaseOfException() {
        ProducerRecord<byte[], byte[]> errorRecord = new ProducerRecord<>(TOPIC, PARTITION,
                "key".getBytes(), "value".getBytes());
        ProductionExceptionHandlerResponse response = globalDLQExceptionHandlerDelegate
                .handle(errorRecord, new TestException());

        assertEquals(ProductionExceptionHandlerResponse.CONTINUE, response);
        verify(producer).send(producedRecord.capture(), any(Callback.class));
        verify(dlqMetadataHandler).withMetadata(eq(errorRecord.headers()), eq(TOPIC), eq(PARTITION),
                any(TestException.class));
        assertEquals("globalDlq", producedRecord.getValue().topic());
        assertArrayEquals("key".getBytes(), producedRecord.getValue().key());
        assertArrayEquals("value".getBytes(), producedRecord.getValue().value());

        assertThat(metrics.globalDlqSentCounter().count(), closeTo(1d, 0.01d));
    }

    @Test
    public void shouldNotPublishToDlqAndLogException() {
        when(kStreamsProcessorConfig.globalDlq().topic()).thenReturn(Optional.of(""));
        when(globalDlqConfig.topic()).thenReturn(Optional.empty());
        ProducerRecord<byte[], byte[]> errorRecord = new ProducerRecord<>(TOPIC, PARTITION,
                "key".getBytes(), "value".getBytes());
        globalDLQExceptionHandlerDelegate.handle(errorRecord, new TestException());
        assertThat(metrics.globalDlqSentCounter().count(), closeTo(0, 0));
    }
}
