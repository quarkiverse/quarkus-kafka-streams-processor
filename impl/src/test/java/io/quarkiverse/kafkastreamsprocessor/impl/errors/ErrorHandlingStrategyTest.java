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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;

import org.junit.jupiter.api.Test;

class ErrorHandlingStrategyTest {
    @Test
    void shouldSendToDlqIfRequested() {
        assertTrue(
                ErrorHandlingStrategy.shouldSendToDlq(ErrorHandlingStrategy.DEAD_LETTER_QUEUE, Optional.of("aTopicName")));
    }

    @Test
    void shouldThrowIfDlqRequestedButNoTopic() {
        Optional<String> noTopic = Optional.empty();
        assertThrows(IllegalStateException.class,
                () -> ErrorHandlingStrategy.shouldSendToDlq(ErrorHandlingStrategy.DEAD_LETTER_QUEUE, noTopic));
    }

    @Test
    void shouldNotSendToDlqWithOtherStrategy() {
        assertFalse(
                ErrorHandlingStrategy.shouldSendToDlq(ErrorHandlingStrategy.FAIL, Optional.of("aTopicName")));
        assertFalse(
                ErrorHandlingStrategy.shouldSendToDlq(ErrorHandlingStrategy.CONTINUE, Optional.of("aTopicName")));
    }
}
