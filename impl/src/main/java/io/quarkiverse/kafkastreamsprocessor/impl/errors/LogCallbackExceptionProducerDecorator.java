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
package io.quarkiverse.kafkastreamsprocessor.impl.errors;

import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;

import lombok.RequiredArgsConstructor;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;

/**
 * {@link Producer} decorator that logs exception that occurs at producing time
 * <p>
 * This decorator is meant to be used with raw kafka {@link Producer} that are not wrapped inside kafka stream where we
 * have trigger of {@link ProductionExceptionHandler} or {@link DeserializationExceptionHandler} in order to react to
 * errors.
 */
@RequiredArgsConstructor
public class LogCallbackExceptionProducerDecorator implements Producer<byte[], byte[]> {
    /**
     * Injection point for composition
     */
    @Delegate(excludes = Excludes.class)
    private final Producer<byte[], byte[]> delegate;

    /**
     * Decorator to impose a {@link Callback} that interrupts exception propagation
     *
     * @param record the record to produce
     * @return the future to get notified when that is done
     */
    @Override
    public Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record) {
        return delegate.send(record, new LogCallback(record, null));
    }

    /**
     * Decorator to impose a {@link Callback} that interrupts exception propagation
     *
     * @param record the record to produce
     * @param callback the given callback that will be decorated to intercepts any Exception
     * @return the future to get notified when that is done
     */
    @Override
    public Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record, Callback callback) {
        return delegate.send(record, new LogCallback(record, callback));
    }

    @Slf4j
    @RequiredArgsConstructor
    private static class LogCallback implements Callback {
        private final ProducerRecord<byte[], byte[]> record;

        private final Callback callback;

        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (exception != null) {
                log.error(
                        "Exception caught during Deserialization, message dropped; topic: {}, partition: {}",
                        record.topic(), record.partition(), exception);
            }
            if (callback != null) {
                callback.onCompletion(metadata, exception);
            }
        }

    }

    private interface Excludes {
        Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record);

        Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record, Callback callback);
    }
}
