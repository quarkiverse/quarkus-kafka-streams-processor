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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.quarkiverse.kafkastreamsprocessor.impl.TestException;

@ExtendWith(MockitoExtension.class)
public class LogCallbackExceptionProducerTest {

    @Mock
    Callback mockCallback;

    LogCallbackExceptionProducerDecorator producer;

    MockProducer<byte[], byte[]> mockProducer;

    @BeforeEach
    void setUp() {
        mockProducer = new MockProducer<>(false, new ByteArraySerializer(), new ByteArraySerializer());
        producer = new LogCallbackExceptionProducerDecorator(mockProducer);
    }

    @Test
    public void shouldPropagateToDelegateCallback() {
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>("topic-dummy", "message".getBytes());

        producer.send(record, mockCallback);

        mockProducer.completeNext();

        verify(mockCallback).onCompletion(any(RecordMetadata.class), eq(null));
    }

    @Test
    public void shouldPropagateExceptionToDelegateCallback() {
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>("topic-dummy", "message".getBytes());

        producer.send(record, mockCallback);

        RuntimeException exception = new TestException();

        mockProducer.errorNext(exception);

        verify(mockCallback).onCompletion(any(RecordMetadata.class), eq(exception));
    }

    @Test
    public void shouldNotFailWithNullCallback() {
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>("topic-dummy", "message".getBytes());

        producer.send(record);

        mockProducer.completeNext();

        verify(mockCallback, never()).onCompletion(any(RecordMetadata.class), eq(null));
    }

}
