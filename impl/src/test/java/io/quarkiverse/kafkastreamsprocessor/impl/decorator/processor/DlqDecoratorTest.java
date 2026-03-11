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
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Optional;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.cloudevents.kafka.CloudEventSerializer;
import io.quarkiverse.kafkastreamsprocessor.api.configuration.Configuration;
import io.quarkiverse.kafkastreamsprocessor.impl.TestException;
import io.quarkiverse.kafkastreamsprocessor.impl.decorator.processor.DlqDecorator.DlqProcessorContextDecorator;
import io.quarkiverse.kafkastreamsprocessor.impl.errors.DlqProducerService;
import io.quarkiverse.kafkastreamsprocessor.spi.properties.DlqConfig;
import io.quarkiverse.kafkastreamsprocessor.spi.properties.InputConfig;
import io.quarkiverse.kafkastreamsprocessor.spi.properties.KStreamsProcessorConfig;

@ExtendWith(MockitoExtension.class)
public class DlqDecoratorTest {

    private static final String FUNCTIONAL_SINK = "functionalSink";
    private static final String TOPIC = "topic";
    private static final int PARTITION = 0;
    private static final String RECORD_KEY = "key";
    private static final String RECORD_VALUE = "value";

    DlqDecorator decorator;

    DlqProcessorContextDecorator<String, String> contextDecorator;

    @Mock
    Processor<String, String, String, String> kafkaProcessor;

    @Mock
    InternalProcessorContext<String, String> context;

    @Mock
    RecordMetadata recordMetadata;

    Headers headers;

    @Mock
    DlqProducerService dlqDelegate;

    @Mock
    Configuration configuration;

    @Mock
    KStreamsProcessorConfig kStreamsProcessorConfig;

    @Mock
    InputConfig inputConfig;

    @Mock
    DlqConfig dlqConfig;

    Record<String, String> testRecord;

    @Captor
    ArgumentCaptor<ConsumerRecord<byte[], byte[]>> captor;

    @BeforeEach
    public void setUp() {
        decorator = new DlqDecorator(Set.of(FUNCTIONAL_SINK), true, dlqDelegate, configuration, kStreamsProcessorConfig);
        decorator.setDelegate(kafkaProcessor);
        decorator.init(context);
        headers = new RecordHeaders();
        testRecord = new Record<>(RECORD_KEY, RECORD_VALUE, 0L, headers);
    }

    @Test
    public void shouldForwardToDlq() {
        doThrow(TestException.class).when(kafkaProcessor).process(testRecord);
        when(context.recordMetadata()).thenReturn(Optional.of(recordMetadata));
        when(context.taskId()).thenReturn(new TaskId(0, PARTITION));
        when(recordMetadata.partition()).thenReturn(PARTITION);
        when(recordMetadata.topic()).thenReturn(TOPIC);
        when(recordMetadata.offset()).thenReturn(0L);
        when(configuration.getSourceKeySerde()).thenAnswer(invocation -> new MySerde());
        when(configuration.getSourceValueSerde()).thenAnswer(invocation -> new MySerde());
        when(kStreamsProcessorConfig.input()).thenReturn(inputConfig);
        when(inputConfig.isCloudEvent()).thenReturn(false);

        assertThrows(TestException.class, () -> decorator.process(testRecord));

        verify(dlqDelegate).sendToDlq(captor.capture(), any(TestException.class),
                any(TaskId.class), eq(false));

        ConsumerRecord<byte[], byte[]> capturedRecord = captor.getValue();
        assertThat(capturedRecord.topic(), equalTo(TOPIC));
        assertThat(capturedRecord.partition(), equalTo(PARTITION));
        assertThat(capturedRecord.offset(), equalTo(0L));
        assertThat(capturedRecord.serializedKeySize(), equalTo(RECORD_KEY.getBytes().length));
        assertThat(capturedRecord.serializedValueSize(), equalTo(RECORD_VALUE.getBytes().length));
        assertThat(capturedRecord.key(), equalTo(RECORD_KEY.getBytes()));
        assertThat(capturedRecord.value(), equalTo(RECORD_VALUE.getBytes()));
        assertThat(capturedRecord.timestamp(), equalTo(testRecord.timestamp()));
        assertThat(capturedRecord.timestampType(), equalTo(TimestampType.CREATE_TIME));
        assertThat(capturedRecord.headers(), equalTo(testRecord.headers()));
    }

    @Test
    public void shouldDelegate() {
        decorator.process(testRecord);
        verify(kafkaProcessor).init(any(DlqProcessorContextDecorator.class));
        verify(kafkaProcessor).process(testRecord);
    }

    @Test
    public void shouldForwardRecordToAllSinks() {
        contextDecorator = new DlqProcessorContextDecorator<>(context, Collections.singleton(FUNCTIONAL_SINK));
        contextDecorator.forward(testRecord);
        verify(context).forward(testRecord, FUNCTIONAL_SINK);
    }

    @Test
    public void shouldForwardKeyValueToAllSinks() {
        contextDecorator = new DlqProcessorContextDecorator<>(context, Collections.singleton(FUNCTIONAL_SINK));
        contextDecorator.forward(RECORD_KEY, RECORD_VALUE);
        verify(context).forward(RECORD_KEY, RECORD_VALUE, To.child(FUNCTIONAL_SINK));
    }

    @Test
    public void shouldDoNothingIfDeactivated() {
        decorator = new DlqDecorator(Set.of(FUNCTIONAL_SINK), false, dlqDelegate, configuration, kStreamsProcessorConfig);
        decorator.setDelegate(kafkaProcessor);
        decorator.init(context);
        decorator.process(testRecord);
        verify(kafkaProcessor).init(context);
        verify(kafkaProcessor).process(testRecord);
    }

    @Test
    public void shouldReturnCloudEventSerializerWhenIsCloudEventTrue() throws Exception {
        when(kStreamsProcessorConfig.input()).thenReturn(inputConfig);
        when(inputConfig.isCloudEvent()).thenReturn(true);
        when(kStreamsProcessorConfig.dlq()).thenReturn(dlqConfig);
        when(dlqConfig.cloudEventSerializerConfig()).thenAnswer(invocation -> new HashMap<String, Object>());

        Serializer<?> serializer = decorator.getSourceValueSerializer(configuration, kStreamsProcessorConfig);
        assertThat(serializer instanceof CloudEventSerializer, equalTo(true));
    }

    @Test
    public void shouldReturnOriginalSerializerWhenIsCloudEventFalse() throws Exception {
        when(kStreamsProcessorConfig.input()).thenReturn(inputConfig);
        when(inputConfig.isCloudEvent()).thenReturn(false);
        when(configuration.getSourceValueSerde()).thenAnswer(invocation -> new MySerde());

        Serializer<?> serializer = decorator.getSourceValueSerializer(configuration, kStreamsProcessorConfig);
        assertThat(serializer instanceof StringSerializer, equalTo(true));
    }

    private static class MySerde implements Serde<String> {
        @Override
        public Serializer<String> serializer() {
            return new StringSerializer();
        }

        @Override
        public Deserializer<String> deserializer() {
            return new StringDeserializer();
        }
    }
}
