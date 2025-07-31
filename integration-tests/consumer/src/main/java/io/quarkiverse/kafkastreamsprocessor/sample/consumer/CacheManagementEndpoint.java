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
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

/**
 * REST endpoint to list and delete events in the {@link InputEventCache}.
 * <p>
 * This endpoint is used for testing purposes only.
 * For the main functionality of this code sample, refer to {@link ConsumerProcessor}.
 */
@Path("/cached-events")
public class CacheManagementEndpoint {
    private final InputEventCache inputEventCache;

    @Inject
    public CacheManagementEndpoint(InputEventCache inputEventCache) {
        this.inputEventCache = inputEventCache;
    }

    /**
     * Lists all stored events in the input event store.
     *
     * @return A string representation of all stored events, or a message indicating no events are stored.
     */
    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public String getCachedEvents() {
        return inputEventCache.describeCachedEvents();
    }

    /**
     * Deletes all stored events in the input event store.
     *
     * @return A message indicating the number of events deleted.
     */
    @DELETE
    @Produces(MediaType.TEXT_PLAIN)
    public String deleteCachedEvents() {
        return String.format("Deleted %d events", inputEventCache.clearCachedEvents());
    }
}
