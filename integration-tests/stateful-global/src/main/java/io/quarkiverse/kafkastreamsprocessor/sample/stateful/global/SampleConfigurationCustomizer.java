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
package io.quarkiverse.kafkastreamsprocessor.sample.stateful.global;

import java.util.ArrayList;
import java.util.List;

import jakarta.enterprise.context.Dependent;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import io.quarkiverse.kafkastreamsprocessor.api.configuration.Configuration;
import io.quarkiverse.kafkastreamsprocessor.api.configuration.ConfigurationCustomizer;
import io.quarkiverse.kafkastreamsprocessor.api.configuration.store.GlobalStoreConfiguration;

@Dependent
public class SampleConfigurationCustomizer implements ConfigurationCustomizer {
    @Override
    public void fillConfiguration(Configuration configuration) {
        List<GlobalStoreConfiguration> globalStoreConfigurations = new ArrayList<>();

        StoreBuilder<KeyValueStore<String, String>> storeBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("store-data"),
                Serdes.String(),
                Serdes.String())
                .withLoggingDisabled();

        globalStoreConfigurations.add(new GlobalStoreConfiguration<String, String>(
                storeBuilder,
                new StringDeserializer(),
                new StringDeserializer(),
                null));

        storeBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("store-data-capital"),
                Serdes.String(),
                Serdes.String())
                .withLoggingDisabled();

        globalStoreConfigurations.add(new GlobalStoreConfiguration<String, String>(
                storeBuilder,
                new StringDeserializer(),
                new StringDeserializer(),
                () -> new CapitalizingStoreProcessor("store-data-capital")));

        configuration.setGlobalStoreConfigurations(globalStoreConfigurations);
    }

    private static class CapitalizingStoreProcessor extends ContextualProcessor<String, String, Void, Void> {

        private final String storeName;

        private KeyValueStore<String, String> store;

        CapitalizingStoreProcessor(String storeName) {
            this.storeName = storeName;
        }

        @Override
        public void init(ProcessorContext<Void, Void> context) {
            super.init(context);
            // Initialize the store
            this.store = context.getStateStore(storeName);
        }

        @Override
        public void process(Record<String, String> record) {
            // Process the record and store it in capitalized form
            store.put(record.key(), record.value().toUpperCase());
        }
    }
}
