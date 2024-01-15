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
package io.quarkiverse.kafkastreamsprocessor.api.configuration;

/**
 * This is the interface allowing customization of the {@link Configuration}. If you want to customize the topology
 * configuration, you need to provide an implementation of this interface, and make it available for CDI injection. By
 * default, an empty implementation provided by the SDK will be picked.
 */
public interface ConfigurationCustomizer {

    /**
     * Method to implement to be able to customize the Kafka Streams behaviour
     *
     * @param configuration the configuration object that exposes a few methods to perform overrides
     */
    void fillConfiguration(Configuration configuration);

}
