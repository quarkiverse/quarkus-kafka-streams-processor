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
package io.quarkiverse.kafkastreamsprocessor.sample.jsonpojo;

import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;

import io.quarkiverse.kafkastreamsprocessor.api.Processor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Processor // <1>
public class PojoProcessor extends ContextualProcessor<String, SamplePojo, String, SamplePojo> { // <2>
    @Override
    public void process(Record<String, SamplePojo> record) { // <3>
        String reversedMsg = new StringBuilder(record.value().getStringField()).reverse().toString();
        log.info("Received value {} sending back {} in response", record.value().getStringField(), reversedMsg);
        SamplePojo pojo = new SamplePojo(reversedMsg, record.value().getNumericalField() + 37,
                !record.value().getBooleanField());
        context().forward(record.withValue(pojo)); // <4>
    }
}
