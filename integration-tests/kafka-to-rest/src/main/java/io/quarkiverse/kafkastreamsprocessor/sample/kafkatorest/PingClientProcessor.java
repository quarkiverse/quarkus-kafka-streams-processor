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
package io.quarkiverse.kafkastreamsprocessor.sample.kafkatorest;

import java.util.Optional;

import jakarta.inject.Inject;

import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.eclipse.microprofile.rest.client.inject.RestClient;

import io.micrometer.core.annotation.Counted;
import io.micrometer.core.annotation.Timed;
import io.quarkiverse.kafkastreamsprocessor.api.Processor;
import io.quarkiverse.kafkastreamsprocessor.sample.message.PingMessage;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Processor
public class PingClientProcessor extends ContextualProcessor<String, PingMessage.Ping, String, PingMessage.Ping> {

    private PingResource client;

    @Inject
    public PingClientProcessor(@RestClient PingResource client) { // <1>
        this.client = client;
    }

    @Override
    @Timed
    @Counted(value = "processedMessageCount", description = "Total number of messages processed")
    public void process(Record<String, PingMessage.Ping> record) {
        Integer partition = null;

        Optional<RecordMetadata> recordMetadata = context().recordMetadata();
        if (recordMetadata.isPresent()) {
            partition = recordMetadata.get().partition();
        }
        log.info("key: {}, value: {}, partition: {}", record.key(), record.value().getMessage(),
                partition);
        String result = client.ping(); // <2>
        context().forward(record
                .withValue(PingMessage.Ping.newBuilder().setMessage(result + " of " + record.value().getMessage()).build()));
    }
}
