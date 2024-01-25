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

import java.time.Duration;

import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import io.quarkiverse.kafkastreamsprocessor.api.Processor;
import io.quarkiverse.kafkastreamsprocessor.sample.message.PingMessage.Ping;
import lombok.extern.slf4j.Slf4j;

/**
 * Test stateful application. Here are the cases:
 * <ul>
 * <li>&lt;key&gt; does not exist in store: Store value in the store and returns <b>Store initialization OK for
 * &lt;key&gt;<b></li>
 * <li>&lt;key&gt; exists in store: Store value in the store and returns <b>Previous value for &lt;key&gt; is
 * &lt;previous-value&gt;</b></li>
 * </ul>
 */
@Slf4j
@Processor // <1>
public class PingProcessor extends ContextualProcessor<String, Ping, String, Ping> {
    private KeyValueStore<String, String> pingData;

    @Override
    public void init(ProcessorContext<String, Ping> context) {
        super.init(context);
        pingData = context.getStateStore("ping-data"); // <2>
        context.schedule(Duration.ofMillis(1L), PunctuationType.STREAM_TIME, new DuplicateValuePunctuator(pingData));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void process(Record<String, Ping> record) {
        log.info("Process the message: {}", record.value().getMessage());

        String previousValue = pingData.get(record.key());
        pingData.put(record.key(), record.value().getMessage());

        if (previousValue == null) {
            context().forward(
                    record.withValue(Ping.newBuilder().setMessage("Store initialization OK for " + record.key()).build()));
        } else {
            context().forward(record.withValue(
                    Ping.newBuilder().setMessage("Previous value for " + record.key() + " is " + previousValue).build()));
        }
    }
}
