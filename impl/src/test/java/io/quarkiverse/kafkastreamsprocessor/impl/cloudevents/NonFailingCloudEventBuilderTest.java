package io.quarkiverse.kafkastreamsprocessor.impl.cloudevents;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.cloudevents.CloudEvent;
import io.cloudevents.SpecVersion;
import io.cloudevents.core.data.BytesCloudEventData;
import io.cloudevents.core.extensions.DistributedTracingExtension;
import io.quarkiverse.kafkastreamsprocessor.spi.properties.KStreamsProcessorConfig;
import io.quarkiverse.kafkastreamsprocessor.spi.properties.OutputConfig;

@ExtendWith(MockitoExtension.class)
class NonFailingCloudEventBuilderTest {
    @Mock
    KStreamsProcessorConfig config;

    @Mock
    OutputConfig outputConfig;

    @BeforeEach
    void setUp() {
        when(config.output()).thenReturn(outputConfig);

        lenient().when(outputConfig.cloudEventsSpecVersion()).thenReturn(SpecVersion.V1);
    }

    @Test
    void specV03() {
        when(outputConfig.cloudEventsSpecVersion()).thenReturn(SpecVersion.V03);

        CloudEvent event = builderWithSourceIdAndTypeFilled().build();

        assertThat(event.getSpecVersion(), equalTo(SpecVersion.V03));
    }

    @Test
    void idSourceTypePassedToBuilder() {
        CloudEvent event = builderWithSourceIdAndTypeFilled().build();

        assertThat(event.getId(), equalTo("AAAA"));
        assertThat(event.getType(), equalTo("BBBB"));
        assertThat(event.getSource().toString(), equalTo("src"));
    }

    @Test
    void idSourceTypeDefaulting() {
        when(outputConfig.cloudEventsSource()).thenReturn(Optional.of(URI.create("src")));
        when(outputConfig.cloudEventsType()).thenReturn(Optional.of("BBBB"));

        CloudEvent event = new NonFailingCloudEventBuilder(config).build();

        assertThat(event.getId(), notNullValue());
        assertThat(event.getType(), equalTo("BBBB"));
        assertThat(event.getSource().toString(), equalTo("src"));
    }

    @Test
    void buildNoSource() {
        when(outputConfig.cloudEventsSource()).thenReturn(Optional.empty());

        assertThrows(IllegalStateException.class, () -> new NonFailingCloudEventBuilder(config).build());
    }

    @Test
    void buildNoType() {
        when(outputConfig.cloudEventsSource()).thenReturn(Optional.of(URI.create("src")));
        when(outputConfig.cloudEventsType()).thenReturn(Optional.empty());

        assertThrows(IllegalStateException.class, () -> new NonFailingCloudEventBuilder(config).build());
    }

    @Test
    void withContextAttributeString() {
        CloudEvent event = builderWithSourceIdAndTypeFilled()
                .withContextAttribute("blabla", "blibli")
                .build();

        assertThat(event.getExtension("blabla"), equalTo("blibli"));
    }

    @Test
    void withContextAttributeURI() {
        CloudEvent event = builderWithSourceIdAndTypeFilled()
                .withContextAttribute("blabla", URI.create("blibli"))
                .build();

        assertThat(event.getExtension("blabla"), instanceOf(URI.class));
        assertThat(event.getExtension("blabla").toString(), equalTo("blibli"));
    }

    @Test
    void withContextAttributeOffsetDateTime() {
        CloudEvent event = builderWithSourceIdAndTypeFilled()
                .withContextAttribute("blabla", OffsetDateTime.MAX)
                .build();

        assertThat(event.getExtension("blabla"), instanceOf(OffsetDateTime.class));
        assertThat(event.getExtension("blabla"), equalTo(OffsetDateTime.MAX));
    }

    @Test
    void withContextAttributeNumber() {
        CloudEvent event = builderWithSourceIdAndTypeFilled()
                .withContextAttribute("blabla", BigDecimal.valueOf(599L))
                .build();

        assertThat(event.getExtension("blabla"), instanceOf(BigDecimal.class));
        assertThat(((BigDecimal) event.getExtension("blabla")).intValue(), equalTo(599));
    }

    @Test
    void withContextAttributeInteger() {
        CloudEvent event = builderWithSourceIdAndTypeFilled()
                .withContextAttribute("blabla", 156)
                .build();

        assertThat(event.getExtension("blabla"), allOf(instanceOf(Integer.class), equalTo(156)));
    }

    @Test
    void withContextAttributeBoolean() {
        CloudEvent event = builderWithSourceIdAndTypeFilled()
                .withContextAttribute("blabla", Boolean.TRUE)
                .build();

        assertThat(event.getExtension("blabla"), instanceOf(Boolean.class));
        assertThat(event.getExtension("blabla"), equalTo(Boolean.TRUE));
    }

    private NonFailingCloudEventBuilder builderWithSourceIdAndTypeFilled() {
        return new NonFailingCloudEventBuilder(config)
                .withId("AAAA")
                .withSource(URI.create("src"))
                .withType("BBBB");
    }

    @Test
    void withContextAttributeByteArray() {
        CloudEvent event = builderWithSourceIdAndTypeFilled()
                .withContextAttribute("blabla", "blibli".getBytes(StandardCharsets.UTF_8))
                .build();

        assertThat(event.getExtension("blabla"), instanceOf(byte[].class));
        assertThat(new String((byte[]) event.getExtension("blabla"), StandardCharsets.UTF_8), equalTo("blibli"));
    }

    @Test
    void withDataSchema() {
        CloudEvent event = builderWithSourceIdAndTypeFilled()
                .withDataSchema(URI.create("blabla"))
                .build();

        assertThat(event.getDataSchema().toString(), equalTo("blabla"));
    }

    @Test
    void withDataContentType() {
        CloudEvent event = builderWithSourceIdAndTypeFilled()
                .withDataContentType("blabla")
                .build();

        assertThat(event.getDataContentType(), equalTo("blabla"));
    }

    @Test
    void withSubject() {
        CloudEvent event = builderWithSourceIdAndTypeFilled()
                .withSubject("blabla")
                .build();

        assertThat(event.getSubject(), equalTo("blabla"));
    }

    @Test
    void withTime() {
        OffsetDateTime now = OffsetDateTime.now();

        CloudEvent event = builderWithSourceIdAndTypeFilled()
                .withTime(now)
                .build();

        assertThat(event.getTime(), equalTo(now));
    }

    @Test
    void withDataByteArray() {
        CloudEvent event = builderWithSourceIdAndTypeFilled()
                .withData("blabla".getBytes(StandardCharsets.UTF_8))
                .build();

        assertThat(new String(event.getData().toBytes(), StandardCharsets.UTF_8), equalTo("blabla"));
    }

    @Test
    void withDataCloudEventData() {
        CloudEvent event = builderWithSourceIdAndTypeFilled()
                .withData(BytesCloudEventData.wrap("blabla".getBytes(StandardCharsets.UTF_8)))
                .build();

        assertThat(new String(event.getData().toBytes(), StandardCharsets.UTF_8), equalTo("blabla"));
    }

    @Test
    void withDataByteArrayWithContentType() {
        CloudEvent event = builderWithSourceIdAndTypeFilled()
                .withData("text/plain", "blabla".getBytes(StandardCharsets.UTF_8))
                .build();

        assertThat(new String(event.getData().toBytes(), StandardCharsets.UTF_8), equalTo("blabla"));
        assertThat(event.getDataContentType(), equalTo("text/plain"));
    }

    @Test
    void withDataByteArrayWithContentTypeAndSchema() {
        CloudEvent event = builderWithSourceIdAndTypeFilled()
                .withData("text/plain", URI.create("schema"), "blabla".getBytes(StandardCharsets.UTF_8))
                .build();

        assertThat(new String(event.getData().toBytes(), StandardCharsets.UTF_8), equalTo("blabla"));
        assertThat(event.getDataContentType(), equalTo("text/plain"));
        assertThat(event.getDataSchema().toString(), equalTo("schema"));
    }

    @Test
    void withDataClientEventDataWithContentType() {
        CloudEvent event = builderWithSourceIdAndTypeFilled()
                .withData("text/plain", BytesCloudEventData.wrap("blabla".getBytes(StandardCharsets.UTF_8)))
                .build();

        assertThat(new String(event.getData().toBytes(), StandardCharsets.UTF_8), equalTo("blabla"));
        assertThat(event.getDataContentType(), equalTo("text/plain"));
    }

    @Test
    void withDataClientEventDataWithContentTypeAndSchema() {
        CloudEvent event = builderWithSourceIdAndTypeFilled()
                .withData("text/plain", URI.create("schema"),
                        BytesCloudEventData.wrap("blabla".getBytes(StandardCharsets.UTF_8)))
                .build();

        assertThat(new String(event.getData().toBytes(), StandardCharsets.UTF_8), equalTo("blabla"));
        assertThat(event.getDataContentType(), equalTo("text/plain"));
        assertThat(event.getDataSchema().toString(), equalTo("schema"));
    }

    @Test
    void withoutData() {
        CloudEvent event = builderWithSourceIdAndTypeFilled()
                .withoutData()
                .build();

        assertThat(event.getData(), nullValue());
    }

    @Test
    void withoutDataSchema() {
        CloudEvent event = builderWithSourceIdAndTypeFilled()
                .withoutDataSchema()
                .build();

        assertThat(event.getDataSchema(), nullValue());
    }

    @Test
    void withoutDataContentType() {
        CloudEvent event = builderWithSourceIdAndTypeFilled()
                .withoutDataContentType()
                .build();

        assertThat(event.getDataContentType(), nullValue());
    }

    @Test
    void withExtensionString() {
        CloudEvent event = builderWithSourceIdAndTypeFilled()
                .withExtension("blabla", "blibli")
                .build();

        assertThat(event.getExtension("blabla"), equalTo("blibli"));
    }

    @Test
    void withExtensionURI() {
        CloudEvent event = builderWithSourceIdAndTypeFilled()
                .withExtension("blabla", URI.create("blibli"))
                .build();

        assertThat(event.getExtension("blabla"), instanceOf(URI.class));
        assertThat(event.getExtension("blabla").toString(), equalTo("blibli"));
    }

    @Test
    void withExtensionOffsetDateTime() {
        CloudEvent event = builderWithSourceIdAndTypeFilled()
                .withExtension("blabla", OffsetDateTime.MAX)
                .build();

        assertThat(event.getExtension("blabla"), instanceOf(OffsetDateTime.class));
        assertThat(event.getExtension("blabla"), equalTo(OffsetDateTime.MAX));
    }

    @Test
    void withExtensionNumber() {
        CloudEvent event = builderWithSourceIdAndTypeFilled()
                .withExtension("blabla", BigDecimal.valueOf(599L))
                .build();

        assertThat(event.getExtension("blabla"), instanceOf(BigDecimal.class));
        assertThat(((BigDecimal) event.getExtension("blabla")).intValue(), equalTo(599));
    }

    @Test
    void withExtensionInteger() {
        CloudEvent event = builderWithSourceIdAndTypeFilled()
                .withExtension("blabla", 156)
                .build();

        assertThat(event.getExtension("blabla"), allOf(instanceOf(Integer.class), equalTo(156)));
    }

    @Test
    void withExtensionBoolean() {
        CloudEvent event = builderWithSourceIdAndTypeFilled()
                .withExtension("blabla", Boolean.TRUE)
                .build();

        assertThat(event.getExtension("blabla"), instanceOf(Boolean.class));
        assertThat(event.getExtension("blabla"), equalTo(Boolean.TRUE));
    }

    @Test
    void withExtensionByteArray() {
        CloudEvent event = builderWithSourceIdAndTypeFilled()
                .withExtension("blabla", "blibli".getBytes(StandardCharsets.UTF_8))
                .build();

        assertThat(event.getExtension("blabla"), instanceOf(byte[].class));
        assertThat(new String((byte[]) event.getExtension("blabla"), StandardCharsets.UTF_8), equalTo("blibli"));
    }

    @Test
    void withExtensionCloudEventExtension() {
        DistributedTracingExtension distributedTracingExtension = new DistributedTracingExtension();
        distributedTracingExtension.setTraceparent("blabla");
        CloudEvent event = builderWithSourceIdAndTypeFilled()
                .withExtension(distributedTracingExtension)
                .build();

        assertThat(event.getExtension("traceparent"), equalTo("blabla"));
    }

    @Test
    void withoutExtensionCloudEventExtension() {
        DistributedTracingExtension distributedTracingExtension = new DistributedTracingExtension();
        distributedTracingExtension.setTraceparent("blabla");

        CloudEvent event = builderWithSourceIdAndTypeFilled()
                .withExtension(distributedTracingExtension)
                .withoutExtension(distributedTracingExtension)
                .build();

        assertThat(event.getExtension("traceparent"), nullValue());
    }

    @Test
    void withoutExtensionWithName() {
        DistributedTracingExtension distributedTracingExtension = new DistributedTracingExtension();
        distributedTracingExtension.setTraceparent("blabla");

        CloudEvent event = builderWithSourceIdAndTypeFilled()
                .withExtension(distributedTracingExtension)
                .withoutExtension("traceparent")
                .build();

        assertThat(event.getExtension("traceparent"), nullValue());
    }

    @Test
    void newBuilder() {
        NonFailingCloudEventBuilder builder1 = builderWithSourceIdAndTypeFilled()
                .withExtension("blabla", "blibli");
        CloudEvent eventWithSecondBuilder = builder1.newBuilder().withExtension("blabla", "blublu").build();
        CloudEvent eventWithFirstBuilder = builder1.build();

        assertThat(eventWithFirstBuilder.getExtension("blabla"), equalTo("blibli"));
        assertThat(eventWithSecondBuilder.getExtension("blabla"), equalTo("blublu"));
    }

    @Test
    void end() {
        CloudEvent event = builderWithSourceIdAndTypeFilled()
                .withExtension("blabla", "blibli")
                .end();

        assertThat(event.getExtension("blabla"), equalTo("blibli"));
        assertThat(event.getSource().toString(), equalTo("src"));
        assertThat(event.getType(), equalTo("BBBB"));
        assertThat(event.getId(), equalTo("AAAA"));
    }

    @Test
    void endWithData() {
        CloudEvent event = builderWithSourceIdAndTypeFilled()
                .end(BytesCloudEventData.wrap("blibli".getBytes(StandardCharsets.UTF_8)));

        assertThat(new String(event.getData().toBytes(), StandardCharsets.UTF_8), equalTo("blibli"));
        assertThat(event.getSource().toString(), equalTo("src"));
        assertThat(event.getType(), equalTo("BBBB"));
        assertThat(event.getId(), equalTo("AAAA"));
    }
}
