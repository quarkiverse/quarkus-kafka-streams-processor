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
package io.quarkiverse.kafkastreamsprocessor.impl.errors;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.charset.StandardCharsets;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

import io.quarkiverse.kafkastreamsprocessor.impl.TestException;
import io.quarkiverse.kafkastreamsprocessor.impl.protocol.KafkaStreamsProcessorHeaders;

public class DlqMetadataHandlerTest {

    DlqMetadataHandler dlqMetadataHandler = new DlqMetadataHandler();

    @Test
    public void metadataShouldBePresent() {
        Headers headers = new RecordHeaders();
        dlqMetadataHandler.addMetadata(headers, "topic", 0,
                new RuntimeException("Reason", new NullPointerException("cause")));
        assertAll(
                () -> assertEquals("Reason", new String(
                        headers.lastHeader(KafkaStreamsProcessorHeaders.DLQ_REASON).value())),
                () -> assertEquals(NullPointerException.class.getName(), new String(
                        headers.lastHeader(KafkaStreamsProcessorHeaders.DLQ_CAUSE).value())),
                () -> assertEquals("0", new String(
                        headers.lastHeader(KafkaStreamsProcessorHeaders.DLQ_PARTITION).value())),
                () -> assertEquals("topic", new String(
                        headers.lastHeader(KafkaStreamsProcessorHeaders.DLQ_TOPIC).value())));
    }

    @Test
    public void testExceptionWithoutMessage() {
        Headers headers = new RecordHeaders();
        dlqMetadataHandler.addMetadata(headers, "topic", 0, new RuntimeException());
        assertNull(headers.lastHeader(KafkaStreamsProcessorHeaders.DLQ_REASON));
    }

    @Test
    public void shouldPropagateExistingHeaders() {
        Headers headers = new RecordHeaders();
        headers.add("headerKey", "headerValue".getBytes(StandardCharsets.UTF_8));

        Headers newHeaders = dlqMetadataHandler.withMetadata(headers, "topic", 0, new TestException());

        assertNotSame(newHeaders, headers);
        assertNotNull(newHeaders.lastHeader("headerKey"));
    }
}
