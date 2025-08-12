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
package io.quarkiverse.kafkastreamsprocessor.sample.consumer;

import jakarta.inject.Inject;

import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;

import io.quarkiverse.kafkastreamsprocessor.api.Processor;
import lombok.extern.slf4j.Slf4j;

/**
 * Pure consumer processor that stores received events in memory.
 */
@Slf4j
@Processor
public class ConsumerProcessor extends ContextualProcessor<String, MyData, Void, Void> {

    private final InputEventCache inputEventCache;

    @Inject
    public ConsumerProcessor(InputEventCache inputEventCache) {
        this.inputEventCache = inputEventCache;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void process(Record<String, MyData> input) {
        log.info("Processing record with key {} and value {}", input.key(), input.value());
        inputEventCache.cacheEvent(input.key(), input.value());
    }
}
