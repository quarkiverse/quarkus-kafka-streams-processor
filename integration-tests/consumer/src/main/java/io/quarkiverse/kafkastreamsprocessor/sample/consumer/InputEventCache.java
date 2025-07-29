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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import jakarta.enterprise.context.ApplicationScoped;

/**
 * A simple in-memory event cache
 * This class is used to store and retrieve events for testing purposes.
 */
@ApplicationScoped
public class InputEventCache {
    Map<String, MyData> events = new ConcurrentHashMap<>();

    public void cacheEvent(String key, MyData value) {
        events.put(key, value);
    }

    /**
     * Lists all cached events in a human-readable format.
     *
     * @return A string representation of all cached events, or a message indicating no events are stored.
     */
    public String describeCachedEvents() {
        if (events.isEmpty()) {
            return "No events cached";
        } else {
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, MyData> entry : events.entrySet()) {
                sb.append("Key: ").append(entry.getKey()).append(", Value: ").append(entry.getValue()).append("\n");
            }
            return sb.toString();
        }
    }

    /**
     * Clear the cache
     *
     * @return The number of events deleted by this operation
     */
    public int clearCachedEvents() {
        int eventCount = events.size();
        events.clear();
        return eventCount;
    }
}
