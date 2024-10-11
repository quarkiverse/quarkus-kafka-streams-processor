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
package io.quarkiverse.kafkastreamsprocessor.impl.decorator.processor;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.quarkiverse.kafkastreamsprocessor.impl.metrics.MockKafkaStreamsProcessorMetrics;
import io.quarkiverse.kafkastreamsprocessor.sample.message.PingMessage.Ping;
import io.quarkus.arc.ArcContainer;

@ExtendWith(MockitoExtension.class)
public class MetricsDecoratorTest {
    @Mock
    Processor<String, Ping, String, Ping> kafkaProcessor;

    MockKafkaStreamsProcessorMetrics metrics = new MockKafkaStreamsProcessorMetrics();

    Ping inputMessage = Ping.newBuilder().setMessage("message").build();

    MetricsDecorator processorDecorator;

    @Mock
    ArcContainer arcContainer;

    @BeforeEach
    public void setUp() {
        processorDecorator = new MetricsDecorator(metrics);
        processorDecorator.setDelegate(kafkaProcessor);
    }

    @Test
    public void shouldDelegateClose() {
        processorDecorator.close();

        verify(kafkaProcessor).close();
    }

    @Test
    void processCalled() {
        Record<String, Ping> record = new Record<>("key1", Ping.newBuilder().setMessage("value1").build(), 0L);

        processorDecorator.process(record);

        verify(kafkaProcessor).process(same(record));
        assertThat(metrics.processorErrorCounter().count(), closeTo(0d, 0.01d));
    }

    @Test
    public void shouldCallCounterMetricProcessorError() {
        Record<String, Ping> record = new Record<>("key", inputMessage, 0L);
        doThrow(RuntimeException.class).when(kafkaProcessor).process(any());

        assertThrows(RuntimeException.class, () -> processorDecorator.process(record));

        assertThat(metrics.processorErrorCounter().count(), closeTo(1d, 0.01d));
    }
}
