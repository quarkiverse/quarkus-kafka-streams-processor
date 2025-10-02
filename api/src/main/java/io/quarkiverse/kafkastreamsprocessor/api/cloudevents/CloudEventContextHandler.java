package io.quarkiverse.kafkastreamsprocessor.api.cloudevents;

import io.cloudevents.CloudEvent;
import io.cloudevents.CloudEventContext;
import io.cloudevents.core.builder.CloudEventBuilder;

/**
 * Handler that can be injected to get access to the {@link CloudEvent} metadata present in the incoming Kafka message,
 * and define the metadata of the outgoing {@link CloudEvent}.
 * <p>
 * The incoming {@link CloudEventContext} of the incoming event is only set if
 * <code>kafkastreamsprocessor.input.is-cloud-event</code> is true.
 * <p>
 * The outgoing {@link CloudEventContext} will only be used if
 * * <code>kafkastreamsprocessor.output.is-cloud-event</code> is true.
 */
public interface CloudEventContextHandler {
    /**
     * The incoming {@link CloudEventContext} of the incoming event.
     * <p>
     * It is null if <code>kafkastreamsprocessor.input.is-cloud-event</code> is false.
     */
    CloudEventContext getIncomingContext();

    /**
     * Setter to set the {@link CloudEventContext} of the outgoing event.
     * <p>
     * It is not used if <code>kafkastreamsprocessor.output.is-cloud-event</code> is false.
     *
     * @param cloudEventContext the cloud event metadata to use when serializing the {@link CloudEvent}
     */
    void setOutgoingContext(CloudEventContext cloudEventContext);

    /**
     * Get a {@link CloudEventBuilder} for the v1 version of the spec that does not validate the fields on build.
     *
     * @return a CloudEvent builder instance of v1 of the spec
     */
    CloudEventBuilder contextBuilder();
}
