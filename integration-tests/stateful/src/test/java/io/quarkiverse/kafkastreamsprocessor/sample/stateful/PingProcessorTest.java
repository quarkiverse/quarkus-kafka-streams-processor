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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.api.MockProcessorContext;
import org.apache.kafka.streams.processor.api.MockProcessorContext.CapturedForward;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.quarkiverse.kafkastreamsprocessor.sample.message.PingMessage.Ping;

public class PingProcessorTest {
    PingProcessor processor = new PingProcessor();

    MockProcessorContext<String, Ping> context = new MockProcessorContext<>();

    KeyValueStore<String, String> store = Stores
            .keyValueStoreBuilder(Stores.inMemoryKeyValueStore("ping-data"), Serdes.String(), Serdes.String())
            .withLoggingDisabled()
            .withCachingDisabled()
            .build();

    @BeforeEach
    public void setup() {
        store.init(context.getStateStoreContext(), store);
        context.addStateStore(store);
        processor.init(context);
    }

    @Test
    public void testKeyNotInStore() {
        sendAndTest("key", "value", "Store initialization OK for key");
        assertThat(store.get("key"), equalTo("value"));
    }

    @Test
    public void testKeyWithPreviousValueInStore() {
        store.put("key", "oldValue");
        sendAndTest("key", "newValue", "Previous value for key is oldValue");
        assertThat(store.get("key"), equalTo("newValue"));
    }

    private void sendAndTest(String key, String message, String expectedMessage) {
        processor.process(new Record<>(key, Ping.newBuilder().setMessage(message).build(), 0L));
        assertThat(context.forwarded(), hasSize(1));
        CapturedForward<?, ?> capturedForward = context.forwarded().get(0);
        assertThat(capturedForward.record().key(), equalTo(key));
        assertThat(((Ping) capturedForward.record().value()).getMessage(), equalTo(expectedMessage));
    }
}
