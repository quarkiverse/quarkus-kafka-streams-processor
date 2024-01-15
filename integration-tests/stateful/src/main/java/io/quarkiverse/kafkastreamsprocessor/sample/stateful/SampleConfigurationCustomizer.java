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
package io.quarkiverse.kafkastreamsprocessor.sample.stateful;

import java.util.ArrayList;
import java.util.List;

import jakarta.enterprise.context.Dependent;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import io.quarkiverse.kafkastreamsprocessor.api.configuration.Configuration;
import io.quarkiverse.kafkastreamsprocessor.api.configuration.ConfigurationCustomizer;
import io.quarkiverse.kafkastreamsprocessor.api.configuration.store.StoreConfiguration;

// tag::customconf[]
@Dependent
public class SampleConfigurationCustomizer implements ConfigurationCustomizer {
    @Override
    public void fillConfiguration(Configuration configuration) {
        List<StoreConfiguration> storeConfigurations = new ArrayList<>();
        // Add a key value store for indexes
        StoreBuilder<KeyValueStore<String, String>> storeBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("ping-data"),
                Serdes.String(),
                Serdes.String());
        storeConfigurations.add(new StoreConfiguration(storeBuilder));
        configuration.setStoreConfigurations(storeConfigurations);
    }
}
// end::customconf[]
