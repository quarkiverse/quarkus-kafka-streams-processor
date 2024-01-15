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

import static io.quarkiverse.kafkastreamsprocessor.sample.stateful.DuplicateValuePunctuator.MAX_SUPPORTED_SIZE;
import static io.quarkiverse.kafkastreamsprocessor.sample.stateful.IsStateStoreContaining.hasKey;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.processor.MockProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DuplicateValuePunctuatorTest {
    MockProcessorContext context = new MockProcessorContext();

    KeyValueStore<String, String> store = Stores
            .keyValueStoreBuilder(Stores.inMemoryKeyValueStore("test"), Serdes.String(), Serdes.String())
            .withCachingDisabled()
            .withLoggingDisabled()
            .build();

    DuplicateValuePunctuator punctuator = new DuplicateValuePunctuator(store);

    @BeforeEach
    void setUp() {
        store.init(context, store);
        context.register(store, null);
    }

    @Test
    void emptyStore() throws Exception {
        punctuator.punctuate(System.currentTimeMillis());
        assertDoesNotThrow(() -> punctuator.punctuate(System.currentTimeMillis()));
    }

    @Test
    void differentValuesStore() throws Exception {
        store.put("key1", "value1");
        store.put("key2", "value2");
        punctuator.punctuate(System.currentTimeMillis());
        store.put("key3", "value3");
        // no duplicate in between --> no exception
        assertDoesNotThrow(() -> punctuator.punctuate(System.currentTimeMillis()));
    }

    @Test
    void duplicateRaiseException() throws Exception {
        store.put("key1", "value1");
        store.put("key2", "value2");
        punctuator.punctuate(System.currentTimeMillis());
        store.put("key3", "value1");
        // duplicate in between --> throw exception and remove dupe
        IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> punctuator.punctuate(System.currentTimeMillis()));
        assertThat(exception.getMessage(), containsString("value1"));
        assertThat(store, not(hasKey("key3")));
    }

    @Test
    void tooManyEntriesSkipping() throws Exception {
        // fill in with more than supported size to force a skip
        for (int i = 0; i < 2 * MAX_SUPPORTED_SIZE; i++) {
            store.put(RandomStringUtils.randomAlphanumeric(4), RandomStringUtils.randomAlphanumeric(4));
        }
        // add duplicates to detect the check is not run
        store.put("key1", "value1");
        store.put("key2", "value1");

        assertDoesNotThrow(() -> punctuator.punctuate(Time.SYSTEM.milliseconds()));
    }
}
