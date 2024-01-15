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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.quarkiverse.kafkastreamsprocessor.impl.TestException;
import io.quarkiverse.kafkastreamsprocessor.impl.TopologyProducer;
import io.quarkiverse.kafkastreamsprocessor.impl.decorator.processor.DlqDecorator.DlqProcessorContextDecorator;
import io.quarkiverse.kafkastreamsprocessor.impl.errors.DlqMetadataHandler;
import io.quarkiverse.kafkastreamsprocessor.impl.metrics.KafkaStreamsProcessorMetrics;
import io.quarkiverse.kafkastreamsprocessor.impl.metrics.MockKafkaStreamsProcessorMetrics;

@ExtendWith(MockitoExtension.class)
public class DlqDecoratorTest {

    private static final String FUNCTIONAL_SINK = "functionalSink";
    private static final String TOPIC = "topic";
    private static final int PARTITION = 0;
    private static final String RECORD_KEY = "key";
    private static final String RECORD_VALUE = "value";

    DlqDecorator<String, String, String, String> decorator;

    DlqProcessorContextDecorator<String, String> contextDecorator;

    @Mock
    Processor<String, String, String, String> kafkaProcessor;

    @Mock
    InternalProcessorContext<String, String> context;

    @Mock
    RecordMetadata recordMetadata;

    Headers headers;

    @Mock
    DlqMetadataHandler dlqMetadataHandler;

    KafkaStreamsProcessorMetrics metrics = new MockKafkaStreamsProcessorMetrics();

    Record<String, String> record;

    @BeforeEach
    public void setUp() {
        decorator = new DlqDecorator<>(kafkaProcessor, Set.of(FUNCTIONAL_SINK), dlqMetadataHandler, metrics, true);
        decorator.init(context);
        headers = new RecordHeaders();
        record = new Record<>(RECORD_KEY, RECORD_VALUE, 0L, headers);
    }

    @Test
    public void shouldForwardToDlq() {
        doThrow(TestException.class).when(kafkaProcessor).process(record);
        when(context.recordMetadata()).thenReturn(Optional.of(recordMetadata));
        when(recordMetadata.partition()).thenReturn(PARTITION);
        when(recordMetadata.topic()).thenReturn(TOPIC);

        assertThrows(TestException.class, () -> decorator.process(record));
        verify(context).forward(eq(record), eq(TopologyProducer.DLQ_SINK_NAME));
        verify(dlqMetadataHandler).addMetadata(eq(headers), eq(TOPIC), eq(PARTITION), any(TestException.class));
        assertThat(metrics.globalDlqSentCounter().count(), closeTo(0d, 0.01d));
        assertThat(metrics.microserviceDlqSentCounter().count(), closeTo(1.0d, 0.1d));
    }

    @Test
    public void shouldDelegate() {
        decorator.process(record);

        verify(kafkaProcessor).init(any(DlqProcessorContextDecorator.class));
        verify(kafkaProcessor).process(eq(record));
    }

    @Test
    public void shouldForwardRecordToAllSinks() {
        contextDecorator = new DlqProcessorContextDecorator<>(context, Collections.singleton(FUNCTIONAL_SINK));
        contextDecorator.forward(record);

        verify(context).forward(eq(record), eq(FUNCTIONAL_SINK));
    }

    @Test
    public void shouldForwardKeyValueToAllSinks() {
        contextDecorator = new DlqProcessorContextDecorator<>(context, Collections.singleton(FUNCTIONAL_SINK));
        contextDecorator.forward(RECORD_KEY, RECORD_VALUE);
        verify(context).forward(eq(RECORD_KEY), eq(RECORD_VALUE), eq(To.child(FUNCTIONAL_SINK)));
    }

    @Test
    public void shouldDoNothingIfDeactivated() {
        decorator = new DlqDecorator<>(kafkaProcessor, Set.of(FUNCTIONAL_SINK), dlqMetadataHandler, metrics, false);

        decorator.init(context);
        decorator.process(record);

        verify(kafkaProcessor).init(same(context));
        verify(kafkaProcessor).process(same(record));
    }
}
