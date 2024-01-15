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
package io.quarkiverse.kafkastreamsprocessor.sample.simple;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;

import java.nio.charset.StandardCharsets;

import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.quarkiverse.kafkastreamsprocessor.api.exception.RetryableException;
import io.quarkiverse.kafkastreamsprocessor.sample.message.PingMessage.Ping;

@ExtendWith(MockitoExtension.class)
public class PingProcessorTest {

    @Mock
    ProcessorContext<String, Ping> context;

    RetryableExceptionTracker retryableExceptionTracker = new RetryableExceptionTracker();

    PingProcessor processor;

    @Captor
    ArgumentCaptor<Record<String, Ping>> captor;

    @BeforeEach
    public void setup() {
        processor = new PingProcessor(retryableExceptionTracker);
        processor.init(context);
    }

    @Test
    public void repliesWithPong() {
        Ping ping = Ping.newBuilder().setMessage("world").build();

        processor.process(new Record<>("key", ping, 0L));

        verify(context).forward(captor.capture());
        Ping pong = captor.getValue().value();

        assertEquals("world", ping.getMessage());
        assertEquals("5", pong.getMessage());
    }

    @Test
    public void repliesWithGeneratedMessage() {
        Ping pong = Ping.newBuilder().setMessage("big message 100").build();

        processor.process(new Record<>("key", pong, 0L));

        verify(context).forward(captor.capture());
        Ping ping = captor.getValue().value();

        assertEquals(100, ping.getMessage().getBytes(StandardCharsets.UTF_8).length);
    }

    @Test
    public void repliesWithError() {
        Ping pong = Ping.newBuilder().setMessage("error").build();

        assertThrows(IllegalArgumentException.class, () -> processor.process(new Record<>("key", pong, 0L)));
    }

    @Test
    public void throwsRetryableIfRequestTimesNotHitYet() {
        retryableExceptionTracker.nbPerformedRetries(5);

        assertThrows(RetryableException.class,
                () -> processor.process(new Record<>("key", Ping.newBuilder().setMessage("retryable-15").build(), 0L)));
    }

    @Test
    public void stopsThrowingRetryableIfRequestTimesHasBeenHit() {
        retryableExceptionTracker.nbPerformedRetries(15);

        processor.process(new Record<>("key", Ping.newBuilder().setMessage("retryable-15").build(), 0L));

        verify(context).forward(captor.capture());
        assertEquals("OK", captor.getValue().value().getMessage());
    }
}
