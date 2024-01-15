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
package io.quarkiverse.kafkastreamsprocessor.impl.decorator.processor;

import java.time.ZonedDateTime;
import java.util.Map;

import lombok.Builder;
import lombok.ToString;

/**
 * Bean used to logged metadata and value of a Kafka record as a last resort in case of exception.
 */
@Builder
@ToString
public class LoggedRecord {
    /**
     * The marshalled value of the record
     */
    private final String value;

    /**
     * The UUID associated to the record
     */
    private final String id;

    /**
     * Timestamp of production
     */
    private final ZonedDateTime time;

    /**
     * The headers received with the message from Kafka
     */
    private final Map<String, String> headers;

    /**
     * The registered application id in Kafka Streams
     */
    private final String appId;

    /**
     * The topic to wish the processor is plugged
     */
    private String topic;

    /**
     * The partition attached to the processor
     */
    private Integer partition;
}
