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
package io.quarkiverse.kafkastreamsprocessor.impl;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import jakarta.enterprise.inject.Instance;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;

import io.quarkiverse.kafkastreamsprocessor.api.decorator.producer.ProducerOnSendInterceptor;
import io.quarkiverse.kafkastreamsprocessor.impl.errors.GlobalDLQProductionExceptionHandler;
import io.quarkiverse.kafkastreamsprocessor.impl.errors.LogAndSendToDlqExceptionHandler;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Delegate;

/**
 * {@link KafkaClientSupplier} decorator to plug the {@link ProducerOnSendInterceptor} around the production of outgoing
 * messages to the actual {@link org.apache.kafka.clients.KafkaClient}.
 */
@RequiredArgsConstructor(access = AccessLevel.MODULE)
public class KafkaClientSupplierDecorator implements KafkaClientSupplier {
    /**
     * Configuration key used by {@link GlobalDLQProductionExceptionHandler} and {@link LogAndSendToDlqExceptionHandler}
     * before requesting a {@link KafkaProducer} to send messages to DLQs.
     */
    public static final String DLQ_PRODUCER = "dlq.producer";

    /**
     * Delegate reference for composition
     */
    @Delegate(excludes = Excludes.class)
    private final KafkaClientSupplier delegate;

    /**
     * Resolver of declared {@link ProducerOnSendInterceptor} instance.
     * <p>
     * Used to wrap the delegate with the interceptors.
     * </p>
     */
    private final Instance<ProducerOnSendInterceptor> interceptors;

    /**
     * Injection constructor.
     *
     * @param interceptors
     *        interceptors to {@link KafkaProducer#send(ProducerRecord)} and
     *        {@link KafkaProducer#send(ProducerRecord, Callback)} respecting their
     *        {@link ProducerOnSendInterceptor#priority()}
     */
    public KafkaClientSupplierDecorator(Instance<ProducerOnSendInterceptor> interceptors) {
        this(new DefaultKafkaClientSupplier(), interceptors);
    }

    /**
     * Decorates {@link KafkaProducer} sends methods with {@link ProducerOnSendInterceptor}.
     * <p>
     * <b>Original documentation:</b>
     * </p>
     * <p>
     * {@inheritDoc}
     */
    @Override
    public Producer<byte[], byte[]> getProducer(Map<String, Object> config) {
        List<ProducerOnSendInterceptor> interceptorList = interceptors.stream().collect(Collectors.toList());
        Collections.sort(interceptorList, Comparator.comparingInt(ProducerOnSendInterceptor::priority));
        return new DecoratedProducer(delegate.getProducer(config), (Boolean) config.get(DLQ_PRODUCER), interceptorList);
    }

    private interface Excludes {
        Producer<byte[], byte[]> getProducer(Map<String, Object> config);
    }

    @RequiredArgsConstructor
    private static class DecoratedProducer implements Producer<byte[], byte[]> {
        @Delegate(excludes = Excludes.class)
        private final Producer<byte[], byte[]> delegate;
        private final Boolean isDlqProducer;
        private final List<ProducerOnSendInterceptor> interceptorList;

        @Override
        public Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record) {
            return delegate.send(intercept(record));
        }

        @Override
        public Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record, Callback callback) {
            return delegate.send(intercept(record), callback);
        }

        private ProducerRecord<byte[], byte[]> intercept(ProducerRecord<byte[], byte[]> record) {
            for (ProducerOnSendInterceptor interceptor : interceptorList) {
                if (!Boolean.TRUE.equals(isDlqProducer) || !interceptor.skipForDLQ()) {
                    record = interceptor.onSend(record);
                }
            }
            return record;
        }

        private interface Excludes {
            Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record);

            Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record, Callback callback);
        }
    }
}
