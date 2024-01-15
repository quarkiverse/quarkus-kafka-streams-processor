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
package io.quarkiverse.kafkastreamsprocessor.impl.decorator.request;

import static io.quarkiverse.kafkastreamsprocessor.sample.message.PingMessage.Ping.newBuilder;

import java.util.UUID;

import jakarta.annotation.PostConstruct;
import jakarta.enterprise.inject.Alternative;
import jakarta.inject.Inject;

import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;

import io.quarkiverse.kafkastreamsprocessor.api.Processor;
import io.quarkiverse.kafkastreamsprocessor.sample.message.PingMessage;

/**
 * Processor manipulating a {@link RequestScopedBean}
 * <p>
 * Forwards a message containing a pair of UUIDs describing the processor and its {@link RequestScopedBean} instances
 * </p>
 */
@Processor
@Alternative
public class RequestScopeConsumerProcessor
        extends ContextualProcessor<String, PingMessage.Ping, String, PingMessage.Ping> {

    @Inject
    RequestScopedBean requestScopedBean;

    /**
     * Processor's UUID
     */
    private String processorUniqueId;

    /**
     * Instantiate the UUID
     */
    @PostConstruct
    public void setup() {
        processorUniqueId = UUID.randomUUID().toString();
    }

    @Override
    public void process(Record<String, PingMessage.Ping> record) {
        String message = processorUniqueId + ":" + requestScopedBean.getUniqueId();
        context().forward(new Record<>(record.key(), newBuilder().setMessage(message).build(), record.timestamp()));
    }
}
