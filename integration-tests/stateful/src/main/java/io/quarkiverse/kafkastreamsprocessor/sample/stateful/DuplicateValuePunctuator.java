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

import java.util.HashSet;
import java.util.Set;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class DuplicateValuePunctuator implements Punctuator {
    public static final int MAX_SUPPORTED_SIZE = 10000;
    private final KeyValueStore<String, String> pingData;

    @Override
    public void punctuate(long punctuationTimestamp) {
        if (pingData.approximateNumEntries() > MAX_SUPPORTED_SIZE) {
            log.warn("Won't search for duplicates as the number of entries in the store seems too high ({} > {}})",
                    pingData.approximateNumEntries(), MAX_SUPPORTED_SIZE);
        } else {
            checkDuplicatesWithCache();
        }
    }

    private void checkDuplicatesWithCache() {
        log.info("Looking for duplicates");
        KeyValueIterator<String, String> it = pingData.all();
        Set<String> valueCache = new HashSet<>();
        Set<String> keysToRemove = new HashSet<>();
        Set<String> valueDuplicates = new HashSet<>();
        while (it.hasNext()) {
            KeyValue<String, String> next = it.next();
            if (!valueCache.add(next.value)) {
                valueDuplicates.add(next.value);
                keysToRemove.add(next.key);
                log.debug("Found key {} that has the same value {} as another key already processed", next.key, next.value);
            }
        }
        log.info("Removing {} keys for which same values were found", keysToRemove.size());
        keysToRemove.forEach(pingData::delete);
        if (!valueDuplicates.isEmpty()) {
            throw new IllegalStateException(
                    "Found values that appear multiple times in state store: " + String.join(", ", valueDuplicates));
        }
    }
}
