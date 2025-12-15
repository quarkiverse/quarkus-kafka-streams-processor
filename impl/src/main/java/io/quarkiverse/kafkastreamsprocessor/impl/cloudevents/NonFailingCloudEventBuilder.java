package io.quarkiverse.kafkastreamsprocessor.impl.cloudevents;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.UUID;

import io.cloudevents.CloudEvent;
import io.cloudevents.CloudEventData;
import io.cloudevents.CloudEventExtension;
import io.cloudevents.SpecVersion;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.rw.CloudEventRWException;
import io.quarkiverse.kafkastreamsprocessor.spi.properties.KStreamsProcessorConfig;
import io.quarkiverse.kafkastreamsprocessor.spi.properties.OutputConfig;

/**
 * Wrapper of {@link CloudEventBuilder} that reimplements {@link CloudEventBuilder#build()} to:
 * <ul>
 * <li>defaulting source to {@link OutputConfig#cloudEventsSource()}</li>
 * <li>defaulting type to {@link OutputConfig#cloudEventsType()}</li>
 * <li>defaulting id to a generated UUID</li>
 * </ul>
 * <p>
 * This allows to avoid IllegalStateException on the origin build method.
 * <p>
 * The {@link CloudEvent} spec version set in {@link OutputConfig#cloudEventsSpecVersion()} is used to choose the
 * corresponding builder implementation.
 *
 * @see CloudEventBuilder
 */
public class NonFailingCloudEventBuilder implements CloudEventBuilder {
    private final KStreamsProcessorConfig config;

    private final CloudEventBuilder delegate;

    private String id;

    private URI source;

    private String type;

    /**
     * Constructor
     *
     * @param config
     *        configuration reference to get the default source and type and the version of the spec to use
     */
    public NonFailingCloudEventBuilder(KStreamsProcessorConfig config) {
        this(config,
                config.output().cloudEventsSpecVersion() == SpecVersion.V1 ? CloudEventBuilder.v1() : CloudEventBuilder.v03());
    }

    private NonFailingCloudEventBuilder(KStreamsProcessorConfig config, CloudEventBuilder builder) {
        this.config = config;
        this.delegate = builder;
    }

    /**
     * {@inheritDoc}
     */
    public NonFailingCloudEventBuilder withId(String id) {
        this.id = id;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    public NonFailingCloudEventBuilder withSource(URI source) {
        this.source = source;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    public NonFailingCloudEventBuilder withType(String type) {
        this.type = type;
        return this;
    }

    /**
     * {@inheritDoc}
     * <p>
     * The build method will not throw {@link IllegalStateException} as the official implementation, so we can default:
     * <ul>
     * <li>source to {@link OutputConfig#cloudEventsSource()}</li>
     * <li>type to {@link OutputConfig#cloudEventsType()}</li>
     * <li>id to a generated UUID</li>
     * </ul>
     */
    @Override
    public CloudEvent build() throws IllegalStateException {
        if (id == null) {
            id = UUID.randomUUID().toString();
        }
        if (source == null) {
            if (config.output().cloudEventsSource().isPresent()) {
                source = config.output().cloudEventsSource().get();
            } else {
                throw new IllegalStateException("No source was set for the outgoing cloud event. " +
                        "Either use withSource(URI) method of the cloud event builder or set the source " +
                        "with the kafkastreamsprocessor.output.cloud-events-source configuration property.");
            }
        }
        if (type == null) {
            if (config.output().cloudEventsType().isPresent()) {
                type = config.output().cloudEventsType().get();
            } else {
                throw new IllegalStateException("No type was set for the outgoing cloud event. " +
                        "Either use withType(String) method of the cloud event builder or set the source " +
                        "with the kafkastreamsprocessor.output.cloud-events-type configuration property.");
            }
        }
        return delegate.withType(type)
                .withSource(source)
                .withId(id)
                .build();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NonFailingCloudEventBuilder withContextAttribute(String s, String s1) throws CloudEventRWException {
        delegate.withContextAttribute(s, s1);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NonFailingCloudEventBuilder withContextAttribute(String name, byte[] value) throws CloudEventRWException {
        delegate.withContextAttribute(name, value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NonFailingCloudEventBuilder withContextAttribute(String name, Boolean value) throws CloudEventRWException {
        delegate.withContextAttribute(name, value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NonFailingCloudEventBuilder withContextAttribute(String name, Integer value) throws CloudEventRWException {
        delegate.withContextAttribute(name, value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NonFailingCloudEventBuilder withContextAttribute(String name, Number value) throws CloudEventRWException {
        delegate.withContextAttribute(name, value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NonFailingCloudEventBuilder withContextAttribute(String name, OffsetDateTime value)
            throws CloudEventRWException {
        delegate.withContextAttribute(name, value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NonFailingCloudEventBuilder withContextAttribute(String name, URI value) throws CloudEventRWException {
        delegate.withContextAttribute(name, value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NonFailingCloudEventBuilder withDataSchema(URI dataSchema) {
        delegate.withDataSchema(dataSchema);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NonFailingCloudEventBuilder withDataContentType(String dataContentType) {
        delegate.withDataContentType(dataContentType);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NonFailingCloudEventBuilder withSubject(String subject) {
        delegate.withSubject(subject);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NonFailingCloudEventBuilder withTime(OffsetDateTime time) {
        delegate.withTime(time);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NonFailingCloudEventBuilder withData(byte[] data) {
        delegate.withData(data);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NonFailingCloudEventBuilder withData(String dataContentType, byte[] data) {
        delegate.withData(dataContentType, data);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NonFailingCloudEventBuilder withData(String dataContentType, URI dataSchema, byte[] data) {
        delegate.withData(dataContentType, dataSchema, data);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NonFailingCloudEventBuilder withData(CloudEventData data) {
        delegate.withData(data);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NonFailingCloudEventBuilder withData(String dataContentType, CloudEventData data) {
        delegate.withData(dataContentType, data);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NonFailingCloudEventBuilder withData(String dataContentType, URI dataSchema, CloudEventData data) {
        delegate.withData(dataContentType, dataSchema, data);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NonFailingCloudEventBuilder withoutData() {
        delegate.withoutData();
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NonFailingCloudEventBuilder withoutDataSchema() {
        delegate.withoutDataSchema();
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NonFailingCloudEventBuilder withoutDataContentType() {
        delegate.withoutDataContentType();
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NonFailingCloudEventBuilder withExtension(String key, String value) {
        delegate.withExtension(key, value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NonFailingCloudEventBuilder withExtension(String key, Number value) {
        delegate.withExtension(key, value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NonFailingCloudEventBuilder withExtension(String key, Boolean value) {
        delegate.withExtension(key, value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NonFailingCloudEventBuilder withExtension(String key, URI value) {
        delegate.withExtension(key, value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NonFailingCloudEventBuilder withExtension(String key, OffsetDateTime value) {
        delegate.withExtension(key, value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NonFailingCloudEventBuilder withExtension(String key, byte[] value) {
        delegate.withExtension(key, value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NonFailingCloudEventBuilder withExtension(CloudEventExtension extension) {
        delegate.withExtension(extension);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NonFailingCloudEventBuilder withoutExtension(String key) {
        delegate.withoutExtension(key);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NonFailingCloudEventBuilder withoutExtension(CloudEventExtension extension) {
        delegate.withoutExtension(extension);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NonFailingCloudEventBuilder newBuilder() {
        return new NonFailingCloudEventBuilder(config, delegate.newBuilder())
                .withSource(source)
                .withType(type)
                .withId(id);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CloudEvent end(CloudEventData cloudEventData) throws CloudEventRWException {
        delegate.withData(cloudEventData);
        return build();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CloudEvent end() throws CloudEventRWException {
        try {
            return build();
        } catch (Exception e) {
            throw CloudEventRWException.newOther(e);
        }
    }
}
