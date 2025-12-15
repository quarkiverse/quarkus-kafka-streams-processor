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
package io.quarkiverse.kafkastreamsprocessor.sample.simple;

import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;

import io.quarkiverse.kafkastreamsprocessor.api.Processor;
import io.quarkiverse.kafkastreamsprocessor.sample.message.Ping;
import lombok.extern.slf4j.Slf4j;

/**
 * Simple processor to test the compatibility with protobuf-v3-generated model classes
 */
@Slf4j
@Processor // <1>
public class PingProcessor extends ContextualProcessor<String, Ping, String, Ping> {

    /**
     * {@inheritDoc}
     */
    @Override
    public void process(Record<String, Ping> ping) { // <3>
        log.info("Process the message: {}", ping.value().getMessage());

        context().forward(
                ping.withValue(Ping.newBuilder().setMessage(Integer.toString(ping.value().getMessage().length())).build()));
    }
}
