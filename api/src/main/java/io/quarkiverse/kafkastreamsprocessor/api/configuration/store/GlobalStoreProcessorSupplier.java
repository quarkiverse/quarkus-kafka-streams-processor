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
package io.quarkiverse.kafkastreamsprocessor.api.configuration.store;

import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;

/**
 * Interface for supplying a processor for a global state store in Kafka Streams.
 *
 * @param <KIn> The type of input keys processed by the processor.
 * @param <VIn> The type of input values processed by the processor.
 */
public interface GlobalStoreProcessorSupplier<KIn, VIn>
        extends ProcessorSupplier<KIn, VIn, Void, Void> {
    @Override
    ContextualProcessor<KIn, VIn, Void, Void> get();
}