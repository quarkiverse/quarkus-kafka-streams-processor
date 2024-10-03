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
package io.quarkiverse.kafkastreamsprocessor.sample.kafkatorest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.quarkiverse.kafkastreamsprocessor.sample.message.PingMessage;

@ExtendWith(MockitoExtension.class)
public class PingClientProcessorTest {

    PingClientProcessor processor;

    @Mock
    PingResource client;

    @Mock
    ProcessorContext<String, PingMessage.Ping> context;

    @Mock
    RecordMetadata recordMetadata;

    @Captor
    ArgumentCaptor<Record<String, PingMessage.Ping>> captor;

    @BeforeEach
    public void setup() {
        processor = new PingClientProcessor(client);
        processor.init(context);
    }

    @Test
    public void replies_with_pong() {
        when(client.ping()).thenReturn("world");
        when(context.recordMetadata()).thenReturn(Optional.of(recordMetadata));

        processor.process(new Record<>("key", PingMessage.Ping.newBuilder().setMessage("hello").build(), 0L));

        verify(context).forward(captor.capture());
        assertEquals("world of hello", captor.getValue().value().getMessage());
    }
}
