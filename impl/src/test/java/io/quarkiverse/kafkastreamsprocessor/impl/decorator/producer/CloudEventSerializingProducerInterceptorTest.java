package io.quarkiverse.kafkastreamsprocessor.impl.decorator.producer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.cloudevents.core.builder.CloudEventBuilder;
import io.quarkiverse.kafkastreamsprocessor.impl.cloudevents.CloudEventContextHandlerImpl;
import io.quarkiverse.kafkastreamsprocessor.spi.properties.KStreamsProcessorConfig;
import io.quarkiverse.kafkastreamsprocessor.spi.properties.OutputConfig;

@ExtendWith(MockitoExtension.class)
class CloudEventSerializingProducerInterceptorTest {
    @Mock
    CloudEventContextHandlerImpl handler;

    @Mock
    KStreamsProcessorConfig config;

    @Mock
    OutputConfig outputConfig;

    CloudEventSerializingProducerInterceptor interceptor;

    ProducerRecord<byte[], byte[]> record;

    @BeforeEach
    void setUp() {
        interceptor = new CloudEventSerializingProducerInterceptor(config, handler);

        record = new ProducerRecord<>("topic", 2, 123456L, "key".getBytes(StandardCharsets.UTF_8),
                "\"value\"".getBytes(StandardCharsets.UTF_8), new RecordHeaders().add("myheader", "headervalue".getBytes(
                        StandardCharsets.UTF_8)));

        when(config.output()).thenReturn(outputConfig);
    }

    @Test
    void noCloudEvent() {
        ProducerRecord<byte[], byte[]> output = interceptor.onSend(record);

        assertThat(output, sameInstance(record));
    }

    @Test
    void noCloudEventOutgoingContext() {
        when(outputConfig.isCloudEvent()).thenReturn(true);

        assertThrows(IllegalStateException.class, () -> interceptor.onSend(record));
    }

    @Test
    void cloudEvent() {
        when(outputConfig.isCloudEvent()).thenReturn(true);
        when(handler.getOutgoingContext()).thenReturn(CloudEventBuilder.v1()
                .withId(UUID.randomUUID().toString())
                .withSource(URI.create("src"))
                .withType("my-type")
                .build());

        ProducerRecord<byte[], byte[]> output = interceptor.onSend(record);

        assertThat(output, not(sameInstance(record)));

        Map<String, String> headers = transformHeaders(output);
        assertThat(headers, hasEntry("ce_type", "my-type"));
        assertThat(headers, hasEntry("ce_source", "src"));
        assertThat(headers, hasEntry(equalTo("ce_id"), notNullValue()));
        assertThat(headers, hasEntry("myheader", "headervalue"));
    }

    private static Map<String, String> transformHeaders(ProducerRecord<byte[], byte[]> output) {
        Map<String, String> headers = new HashMap<>();
        output.headers().forEach(h -> headers.put(h.key(), new String(h.value(), StandardCharsets.UTF_8)));
        return headers;
    }

    @Test
    void cloudEventStructured() {
        when(outputConfig.isCloudEvent()).thenReturn(true);
        when(handler.getOutgoingContext()).thenReturn(CloudEventBuilder.v1()
                .withId(UUID.randomUUID().toString())
                .withSource(URI.create("src"))
                .withType("my-type")
                .build());
        when(outputConfig.cloudEventSerializerConfig()).thenReturn(Map.of("cloudevents.serializer.encoding",
                "STRUCTURED", "cloudevents.serializer.event_format", "application/cloudevents+json"));

        ProducerRecord<byte[], byte[]> output = interceptor.onSend(record);

        assertThat(output, not(sameInstance(record)));
        Map<String, String> headers = transformHeaders(output);
        assertThat(headers, not(hasKey(startsWith("ce_"))));
        assertThat(headers, hasEntry("myheader", "headervalue"));
        String payload = new String(output.value(), StandardCharsets.UTF_8);
        assertThat(payload, allOf(
                containsString("\"id\":"),
                containsString("\"type\":\"my-type\""),
                containsString("\"source\":\"src\""),
                containsString("\"data\":\"value\"")));
    }

    @Test
    void incorrectEncoding() {
        when(outputConfig.isCloudEvent()).thenReturn(true);
        when(handler.getOutgoingContext()).thenReturn(CloudEventBuilder.v1()
                .withId(UUID.randomUUID().toString())
                .withSource(URI.create("src"))
                .withType("my-type")
                .build());
        when(outputConfig.cloudEventSerializerConfig()).thenReturn(Map.of("cloudevents.serializer.encoding",
                "NONEXISTANT"));

        assertThrows(IllegalArgumentException.class, () -> interceptor.onSend(record));
    }

    @Test
    void incorrectDataFormat() {
        when(outputConfig.isCloudEvent()).thenReturn(true);
        when(handler.getOutgoingContext()).thenReturn(CloudEventBuilder.v1()
                .withId(UUID.randomUUID().toString())
                .withSource(URI.create("src"))
                .withType("my-type")
                .build());
        when(outputConfig.cloudEventSerializerConfig()).thenReturn(Map.of("cloudevents.serializer.encoding",
                "STRUCTURED", "cloudevents.serializer.event_format", "text/plain"));

        assertThrows(IllegalArgumentException.class, () -> interceptor.onSend(record));
    }
}
