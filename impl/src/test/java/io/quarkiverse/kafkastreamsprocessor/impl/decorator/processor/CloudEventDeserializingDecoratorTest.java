package io.quarkiverse.kafkastreamsprocessor.impl.decorator.processor;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.nio.charset.StandardCharsets;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.cloudevents.CloudEvent;
import io.cloudevents.CloudEventContext;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.quarkiverse.kafkastreamsprocessor.api.configuration.Configuration;
import io.quarkiverse.kafkastreamsprocessor.impl.cloudevents.CloudEventContextHandlerImpl;
import io.quarkiverse.kafkastreamsprocessor.spi.properties.InputConfig;
import io.quarkiverse.kafkastreamsprocessor.spi.properties.KStreamsProcessorConfig;

@ExtendWith(MockitoExtension.class)
class CloudEventDeserializingDecoratorTest {
    @Mock
    KStreamsProcessorConfig microprofileConfig;

    @Mock
    InputConfig inputConfig;

    @Mock
    Configuration configuration;

    @Mock
    CloudEventContextHandlerImpl handler;

    CloudEventDeserializingDecorator decorator;

    @Mock
    Processor<String, String, String, String> delegate;

    @Mock
    ProcessorContext<String, String> context;

    @BeforeEach
    void setUp() {
        decorator = new CloudEventDeserializingDecorator(microprofileConfig, configuration, handler);
        decorator.setDelegate(delegate);

        when(microprofileConfig.input()).thenReturn(inputConfig);
    }

    @Test
    void doesNothingIfNotCloudEvent() {
        Record<String, String> record = new Record<>("key", "value", 123456L);

        decorator.init(context);
        decorator.process(record);

        verify(delegate).init(context);
        verify(delegate).process(record);
    }

    @Test
    void throwsIfRecordValueTypeIsNotCloudEvent() {
        Record<String, String> record = new Record<>("key", "value", 123456L);
        when(inputConfig.isCloudEvent()).thenReturn(true);

        decorator.init(context);
        assertThrows(IllegalStateException.class, () -> decorator.process(record));
    }

    @Test
    void noRecordMetadata() {
        Record<String, CloudEvent> record = new Record<>("key",
                CloudEventBuilder.v1().withId("id").withType("type").withSource(URI.create("source"))
                        .withData("blabla".getBytes(StandardCharsets.UTF_8)).build(),
                123456L);
        when(inputConfig.isCloudEvent()).thenReturn(true);
        when(configuration.getSourceValueSerde()).thenReturn(new MySerde());

        decorator.init(context);
        decorator.process(record);

        ArgumentCaptor<Record> recordCaptor = ArgumentCaptor.forClass(Record.class);
        verify(delegate).process(recordCaptor.capture());
        assertThat(recordCaptor.getValue().value(), equalTo("blabla"));

        ArgumentCaptor<CloudEventContext> incomingContextCaptor = ArgumentCaptor.forClass(CloudEventContext.class);
        verify(handler).setIncomingContext(incomingContextCaptor.capture());
        assertThat(incomingContextCaptor.getValue().getSource().toString(), equalTo("source"));
        assertThat(incomingContextCaptor.getValue().getType(), equalTo("type"));
        assertThat(incomingContextCaptor.getValue().getId(), equalTo("id"));
    }

    private static class MySerde implements Serde {
        @Override
        public Serializer serializer() {
            return new StringSerializer();
        }

        @Override
        public Deserializer deserializer() {
            return new StringDeserializer();
        }
    }
}
