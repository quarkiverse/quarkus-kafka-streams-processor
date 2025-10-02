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
package io.quarkiverse.kafkastreamsprocessor.impl.decorator.producer;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.apache.kafka.clients.producer.ProducerRecord;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.core.data.BytesCloudEventData;
import io.cloudevents.kafka.CloudEventSerializer;
import io.quarkiverse.kafkastreamsprocessor.api.decorator.producer.ProducerInterceptorPriorities;
import io.quarkiverse.kafkastreamsprocessor.api.decorator.producer.ProducerOnSendInterceptor;
import io.quarkiverse.kafkastreamsprocessor.impl.cloudevents.CloudEventContextHandlerImpl;
import io.quarkiverse.kafkastreamsprocessor.spi.properties.KStreamsProcessorConfig;

/**
 * Producer interceptor that wraps the byte array of the payload into a {@link CloudEvent} structure, before serializing
 * it to byte array again and use that as message payload.
 * <p>
 * The key/values stored under <code>kafkastreamsprocessorCloudEventSerializingProducerInterceptor</code>
 */
@ApplicationScoped
public class CloudEventSerializingProducerInterceptor implements ProducerOnSendInterceptor {
    private final KStreamsProcessorConfig kStreamsProcessorConfig;
    private final CloudEventContextHandlerImpl contextHandler;
    private CloudEventSerializer cloudEventSerializer;

    @Inject
    public CloudEventSerializingProducerInterceptor(KStreamsProcessorConfig kStreamsProcessorConfig,
            CloudEventContextHandlerImpl contextHandler) {
        this.kStreamsProcessorConfig = kStreamsProcessorConfig;
        this.contextHandler = contextHandler;
    }

    @Override
    public ProducerRecord<byte[], byte[]> onSend(ProducerRecord<byte[], byte[]> record) {
        if (kStreamsProcessorConfig.output().isCloudEvent()) {
            if (contextHandler.getOutgoingContext() == null) {
                throw new IllegalStateException("CloudEventContextHandler.setOutoingContext has not been called "
                        + " in the Processor. The applicative code is responsible to define the metadata of the outgoing"
                        + " CloudEvent before forwarding the outgoing message.");
            }
            BytesCloudEventData wrapped = BytesCloudEventData.wrap(record.value());
            CloudEvent cloudEvent = CloudEventBuilder.fromContext(contextHandler.getOutgoingContext())
                    .withData(wrapped)
                    .build();
            byte[] serializedCloudEvent = serializer().serialize(record.topic(), record.headers(), cloudEvent);
            return new ProducerRecord<>(record.topic(), record.partition(), record.timestamp(), record.key(),
                    serializedCloudEvent, record.headers());
        }
        return record;
    }

    // caching the serializer
    private CloudEventSerializer serializer() {
        if (cloudEventSerializer == null) {
            synchronized (this) {
                if (cloudEventSerializer == null) {
                    cloudEventSerializer = new CloudEventSerializer();
                    cloudEventSerializer.configure(kStreamsProcessorConfig.output().cloudEventSerializerConfig(), false);
                }
            }
        }
        return cloudEventSerializer;
    }

    @Override
    public int priority() {
        return ProducerInterceptorPriorities.CLOUD_EVENT_SERIALIZATION;
    }
}
