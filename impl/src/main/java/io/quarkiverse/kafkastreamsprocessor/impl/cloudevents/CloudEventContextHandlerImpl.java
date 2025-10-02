package io.quarkiverse.kafkastreamsprocessor.impl.cloudevents;

import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;

import io.cloudevents.CloudEventContext;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.quarkiverse.kafkastreamsprocessor.api.cloudevents.CloudEventContextHandler;
import io.quarkiverse.kafkastreamsprocessor.spi.properties.KStreamsProcessorConfig;
import lombok.Getter;
import lombok.Setter;

@RequestScoped
@Getter
@Setter
public class CloudEventContextHandlerImpl implements CloudEventContextHandler {
    private final KStreamsProcessorConfig config;

    private CloudEventContext incomingContext;

    private CloudEventContext outgoingContext;

    @Inject
    public CloudEventContextHandlerImpl(KStreamsProcessorConfig config) {
        this.config = config;
    }

    public CloudEventBuilder contextBuilder() {
        return new NonFailingCloudEventBuilder(config);
    }

}
