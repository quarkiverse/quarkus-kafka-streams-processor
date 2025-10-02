package io.quarkiverse.kafkastreamsprocessor.impl.cloudevents;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.when;

import java.net.URI;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.cloudevents.CloudEvent;
import io.cloudevents.SpecVersion;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.quarkiverse.kafkastreamsprocessor.spi.properties.KStreamsProcessorConfig;
import io.quarkiverse.kafkastreamsprocessor.spi.properties.OutputConfig;

@ExtendWith(MockitoExtension.class)
class CloudEventContextHandlerImplTest {
    @Mock
    KStreamsProcessorConfig config;

    @Mock
    OutputConfig outputConfig;

    CloudEventContextHandlerImpl handler;

    @BeforeEach
    void setUp() {
        handler = new CloudEventContextHandlerImpl(config);

        when(config.output()).thenReturn(outputConfig);
        when(outputConfig.cloudEventsSpecVersion()).thenReturn(SpecVersion.V1);
    }

    @Test
    void contextBuilder() {
        CloudEventBuilder builder = handler.contextBuilder();
        assertThat(builder, instanceOf(NonFailingCloudEventBuilder.class));
        CloudEvent event = builder.withSource(URI.create("blabla"))
                .withType("blabla")
                .build();
        assertThat(event.getSpecVersion(), equalTo(SpecVersion.V1));
    }

}