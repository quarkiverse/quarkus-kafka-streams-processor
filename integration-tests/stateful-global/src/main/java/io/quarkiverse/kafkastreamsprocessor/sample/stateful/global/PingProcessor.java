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

import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import io.quarkiverse.kafkastreamsprocessor.api.Processor;
import io.quarkiverse.kafkastreamsprocessor.sample.message.PingMessage.Ping;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Processor
public class PingProcessor extends ContextualProcessor<String, Ping, String, Ping> {
    private KeyValueStore<String, String> storeData;

    private KeyValueStore<String, String> storeDataCapital;

    @Override
    public void init(ProcessorContext<String, Ping> context) {
        super.init(context);
        storeData = context.getStateStore("store-data");

        storeDataCapital = context.getStateStore("store-data-capital");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void process(Record<String, Ping> record) {
        String storedValue = storeData.get(record.key());

        String storedValueCapital = storeDataCapital.get(record.key());

        log.info("Retrieve the value for key from global data store: {}", record.key());

        if (storedValue == null && storedValueCapital == null) {
            context().forward(record.withValue(
                    Ping.newBuilder().setMessage("Stored value for " + record.key() + " is null").build()));
        } else {
            context().forward(record.withValue(
                    Ping.newBuilder().setMessage("Stored value for " + record.key() + " is " + storedValue
                            + " and capitalized value is " + storedValueCapital)
                            .build()));
        }

    }
}
