package io.quarkiverse.kafkastreamsprocessor.impl.decorator.processor;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

import io.cloudevents.CloudEvent;
import io.quarkiverse.kafkastreamsprocessor.api.cloudevents.CloudEventContextHandler;
import io.quarkiverse.kafkastreamsprocessor.api.configuration.Configuration;
import io.quarkiverse.kafkastreamsprocessor.api.decorator.processor.AbstractProcessorDecorator;
import io.quarkiverse.kafkastreamsprocessor.api.decorator.processor.ProcessorDecoratorPriorities;
import io.quarkiverse.kafkastreamsprocessor.impl.cloudevents.CloudEventContextHandlerImpl;
import io.quarkiverse.kafkastreamsprocessor.spi.properties.InputConfig;
import io.quarkiverse.kafkastreamsprocessor.spi.properties.KStreamsProcessorConfig;

/**
 * Processor decorator that will deserialize the data part of the received {@link CloudEvent}.
 * <p>
 * This is done only if {@link InputConfig#isCloudEvent()} is true (corresponds to the
 * <code>kafkastreamsprocessor.input.is-cloud-event</code> configuration property).
 * <p>
 * The deserializer used to deserialize the payload (in the form of a byte array) is retrieved from the resolved
 * {@link Configuration} object.
 */
@Dependent
@Priority(ProcessorDecoratorPriorities.CLOUD_EVENT_DESERIALIZING)
public class CloudEventDeserializingDecorator extends AbstractProcessorDecorator {
    private final KStreamsProcessorConfig kStreamsProcessorConfig;
    private final Configuration configuration;
    private final CloudEventContextHandlerImpl contextHandler;

    private ProcessorContext context;

    /**
     * Constructor for injection
     *
     * @param kStreamsProcessorConfig
     *        the current config mapping for the library
     * @param configuration
     *        the resolved {@link Configuration} object after applications of the
     *        {@link io.quarkiverse.kafkastreamsprocessor.api.configuration.ConfigurationCustomizer}
     * @param contextHandler
     *        the request-scope context handler data structure
     */
    @Inject
    public CloudEventDeserializingDecorator(KStreamsProcessorConfig kStreamsProcessorConfig,
            Configuration configuration, CloudEventContextHandlerImpl contextHandler) {
        this.kStreamsProcessorConfig = kStreamsProcessorConfig;
        this.configuration = configuration;
        this.contextHandler = contextHandler;
    }

    /**
     * Override init to capture the {@link ProcessorContext} instance
     *
     * @param context
     *        the {@link ProcessorContext} instance
     */
    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        getDelegate().init(context);
    }

    /**
     * If <code>kafkastreamsprocessor.input.is-cloud-event</code> is true, we preprocess the {@link Record} and its
     * {@link CloudEvent} value to:
     * <ul>
     * <li>store the extract metadata of the incoming event in {@link CloudEventContextHandler}</li>
     * <li>get the byte[] data of the event and deserialize the payload into the expected POJO type setup on the
     * processor.</li>
     * </ul>
     * <p>
     * A new record is forwarded with the deserialized payload as value, to match the Processor type signature.
     *
     * @param record
     *        the record to process with as value a CloudEvent instance in case
     *        <code>kafkastreamsprocessor.input.is-cloud-event</code> is true
     */
    @Override
    public void process(Record record) {
        if (kStreamsProcessorConfig.input().isCloudEvent()) {
            if (record.value() instanceof CloudEvent cloudEvent) {
                Deserializer<?> deserializer = configuration.getSourceValueSerde().deserializer();
                String topic = context.recordMetadata().isPresent() ? context.recordMetadata().get().topic() : null;
                Object deserialized = deserializer.deserialize(topic, record.headers(), cloudEvent.getData().toBytes());
                contextHandler.setIncomingContext(cloudEvent);
                getDelegate().process(record.withValue(deserialized));
            } else {
                throw new IllegalStateException("Cloud event activated, should have received a value of type CloudEvent");
            }
        } else {
            getDelegate().process(record);
        }
    }
}
