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
package io.quarkiverse.kafkastreamsprocessor.impl.decorator.request;

import java.util.UUID;

import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.RequestScoped;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Simple {@link RequestScoped} object holding an ID unique to the instance
 */
@RequestScoped
@Slf4j
public class RequestScopedBean {
    /**
     * Instance's UUID
     * <p>
     * Must be manipulated through accessors to not mess with the injection mechanism
     * </p>
     */
    @Getter
    private String uniqueId;

    /**
     * Instantiate the UUID
     */
    @PostConstruct
    public void setup() {
        uniqueId = UUID.randomUUID().toString();
    }
}
