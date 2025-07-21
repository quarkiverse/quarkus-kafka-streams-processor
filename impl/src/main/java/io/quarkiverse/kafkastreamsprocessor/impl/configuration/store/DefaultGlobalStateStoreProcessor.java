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
package io.quarkiverse.kafkastreamsprocessor.impl.configuration.store;

import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * A default implementation of a global state store processor for Kafka Streams.
 */
public class DefaultGlobalStateStoreProcessor extends ContextualProcessor<Object, Object, Void, Void> {

    /**
     * The name of the state store to be used by this processor.
     */
    private final String storeName;

    /**
     * The state store instance used to store key-value pairs.
     */
    private KeyValueStore<Object, Object> store;

    /**
     * Constructs a new DefaultGlobalStateStoreProcessor.
     *
     * @param storeName The name of the state store to be used.
     */
    public DefaultGlobalStateStoreProcessor(String storeName) {
        this.storeName = storeName;
    }

    /**
     * Initializes the processor with the given context.
     *
     * @param context The processor context providing access to the state store and other resources.
     */
    @Override
    public void init(ProcessorContext<Void, Void> context) {
        super.init(context);
        // Initialize the store
        this.store = context.getStateStore(storeName);
    }

    /**
     * Processes a record by storing its key-value pair in the state store.
     *
     * @param record The record to be processed.
     */
    @Override
    public void process(Record<Object, Object> record) {
        // put the record in the store
        store.put(record.key(), record.value());
    }
}
