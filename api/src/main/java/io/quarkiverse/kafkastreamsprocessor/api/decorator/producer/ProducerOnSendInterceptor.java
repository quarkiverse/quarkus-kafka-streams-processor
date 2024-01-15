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
package io.quarkiverse.kafkastreamsprocessor.api.decorator.producer;

import java.util.Map;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * Interface to extend to by able to decorate the production of the response message to the outgoing topic.
 */
public interface ProducerOnSendInterceptor extends ProducerInterceptor<byte[], byte[]> {
    /**
     * By default, if not overriden, the interceptor has the following priority.
     */
    int DEFAULT_PRIORITY = 100;

    /**
     * Override this method to finely tune the order of execution of any interceptor you implement.
     *
     * @return the custom priority you want to assign. A number between 0 and {@link Integer#MAX_VALUE}.
     */
    default int priority() {
        return DEFAULT_PRIORITY;
    }

    /**
     * Tells the framework whether to skip decoration or not, with this decorator class, of the
     * {@link org.apache.kafka.clients.producer.KafkaProducer} that sends messages to the DLQ.
     * <p>
     * This has to do with the fact that the decorator itself could be a source of exceptions. Those exceptions if
     * managed, could trigger a message production to the DLQ. If the producer to the DLQ is also decorated, we could end
     * up in an infinite loop.
     * </p>
     * <p>
     * Explanation:
     * </p>
     * <ol>
     * <li>Incoming message</li>
     * <li>MyDecorator</li>
     * <li>raise exception</li>
     * <li>caught, sending to DLQ</li>
     * <li>MyDecorator</li>
     * <li>raise exception</li>
     * <li>loop to point 4... etc.</li>
     * </ol>
     * <p>
     * With this boolean field, we can prevent the passage through MyDecorator when going to a DLQ.
     * </p>
     *
     * @return whether to skip decoration or not, with this decorator class, of the
     *         {@link org.apache.kafka.clients.producer.KafkaProducer} that sends messages to the DLQ
     */
    default boolean skipForDLQ() {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    default void configure(Map<String, ?> config) {

    }

    /**
     * {@inheritDoc}
     */
    @Override
    default void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        // do nothing
    }

    /**
     * {@inheritDoc}
     */
    @Override
    default void close() {
        // do nothing
    }
}
