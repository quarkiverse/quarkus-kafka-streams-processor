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
package io.quarkiverse.kafkastreamsprocessor.sample.multioutput;

import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;

import io.quarkiverse.kafkastreamsprocessor.api.Processor;
import io.quarkiverse.kafkastreamsprocessor.sample.message.PingMessage.Ping;
import lombok.extern.slf4j.Slf4j;

@Processor
@Slf4j
public class PingProcessor extends ContextualProcessor<String, Ping, String, Ping> {
    public static final String MULTIPLE_OUTPUT_TEST_FEATURE = "multiple.output.test.feature";

    @Override
    public void process(Record<String, Ping> record) {
        if ("error".equals(record.value().getMessage())) {
            // Functional error event
            Ping pang = Ping.newBuilder().setMessage("PANG!").build();
            context().forward(record.withValue(pang), "pang");
        } else {
            // Count the number of characters in input message
            int characterCount = record.value().getMessage().length();
            log.debug("Character count is {}", characterCount);
            Ping pong = Ping.newBuilder().setMessage(String.valueOf(characterCount)).build();
            context().forward(record.withValue(pong), "pong");
        }
    }
}
