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

import jakarta.enterprise.context.Dependent;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.internals.ProcessorAdapter;

import lombok.experimental.Delegate;

/**
 * CDI-annotated element to help support Kafka Streams' {@link org.apache.kafka.streams.processor.Processor} interface
 * from previous Kafka Streams versions.
 *
 * @param <K>
 *        unique key type for incoming and outgoing messages
 * @param <V>
 *        unique value type for incoming and outgoing messages
 * @deprecated til Kafka finally removes the {@link org.apache.kafka.streams.processor.Processor} interface
 */
@Dependent
@Deprecated
public class Kafka2ProcessorAdapter<K, V> implements Processor<K, V, K, V> {
    /**
     * The {@link ProcessorAdapter} mapped by this bean
     */
    @Delegate
    private Processor<K, V, K, V> processor;

    /**
     * Injection point for the Kafka 2.x processor
     *
     * @param kafka2Processor the original Kafka version 2.x processor
     */
    public void adapt(org.apache.kafka.streams.processor.Processor<K, V> kafka2Processor) {
        this.processor = ProcessorAdapter.adapt(kafka2Processor);
    }
}
