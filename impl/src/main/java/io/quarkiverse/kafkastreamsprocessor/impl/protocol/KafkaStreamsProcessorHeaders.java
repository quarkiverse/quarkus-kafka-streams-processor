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
package io.quarkiverse.kafkastreamsprocessor.impl.protocol;

import org.apache.kafka.streams.processor.api.Processor;

/**
 * Kafka metadata header names
 * <p>
 * <b>Note:</b> the dead-letter-queue-related headers (prefixed with <code>DLQ_</code>) have their equivalent in
 * <code>smallrye-reactive-messaging-kafka</code>. The constants in this class are to keep up with their Smallrye's equivalent.
 * </p>
 *
 * @see <a href=
 *      "https://github.com/smallrye/smallrye-reactive-messaging/blob/main/smallrye-reactive-messaging-kafka/src/main/java/io/smallrye/reactive/messaging/kafka/fault/KafkaDeadLetterQueue.java#L50">Dead-letter
 *      queue headers in <code>smallrye-reactive-messaging-kafka</code></a>
 */
public final class KafkaStreamsProcessorHeaders {
    /**
     * Technical header to trace any outgoing messages to their incoming messages.
     * <p>
     * It is under other used to match a "request" with its "reply" in a bridge from transactions to events, like HTTP to
     * Kafka.
     * </p>
     * <p>
     * This constant is not effectively used to propagate its value, indeed Kafka Streams does propagate headers by
     * default on a {@link Processor}.
     * </p>
     *
     * @see <a href=
     *      "https://github.com/smallrye/smallrye-reactive-messaging/blob/e5e423458003a7d29ac4e3c5018ce7bbea930eed/smallrye-reactive-messaging-kafka/src/main/java/io/smallrye/reactive/messaging/kafka/reply/KafkaRequestReply.java#L29">KafkaRequestReply
     *      in smallrye-reactive-messaging-kafka</a>
     */
    public static final String UUID = "uuid";

    /**
     * W3C tracing header. It is propagated by the opentelemetry.
     */
    public static final String W3C_TRACE_ID = "traceparent";

    /**
     * W3C tracing baggage. It is propagated by the opentelemetry if configured to do so.
     */
    public static final String W3C_BAGGAGE = "baggage";

    /**
     * The reason of the failure.
     */
    public static final String DLQ_REASON = "dead-letter-reason";

    /**
     * The cause of the failure if any
     */
    public static final String DLQ_CAUSE = "dead-letter-cause";

    /**
     * The original topic of the record
     */
    public static final String DLQ_TOPIC = "dead-letter-topic";

    /**
     * The original partition of the record (integer mapped to String)
     */
    public static final String DLQ_PARTITION = "dead-letter-partition";

    private KafkaStreamsProcessorHeaders() {

    }
}
