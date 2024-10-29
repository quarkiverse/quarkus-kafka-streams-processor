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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import io.quarkiverse.kafkastreamsprocessor.spi.properties.DlqConfig;
import io.quarkiverse.kafkastreamsprocessor.spi.properties.KStreamsProcessorConfig;
import io.quarkus.test.Mock;

@ExtendWith({ MockitoExtension.class })
class ErrorHandlingStrategyTest {

    @Mock
    KStreamsProcessorConfig extensionConfiguration;

    @Mock
    DlqConfig dlqConfig;

    @BeforeEach
    void setUp() {
        when(extensionConfiguration.dlq()).thenReturn(dlqConfig);
    }

    @Test
    void shouldSendToDlqIfRequested() {
        when(dlqConfig.topic()).thenReturn(Optional.of("aTopicName"));

        assertTrue(
                new ErrorHandlingStrategy(ErrorHandlingStrategy.DEAD_LETTER_QUEUE, extensionConfiguration).shouldSendToDlq());
    }

    @Test
    void shouldThrowIfDlqRequestedButNoTopic() {
        Optional<String> noTopic = Optional.empty();
        when(dlqConfig.topic()).thenReturn(noTopic);

        assertThrows(IllegalStateException.class,
                () -> new ErrorHandlingStrategy(ErrorHandlingStrategy.DEAD_LETTER_QUEUE, extensionConfiguration)
                        .shouldSendToDlq());
    }

    @Test
    void shouldNotSendToDlqWithOtherStrategy() {
        when(dlqConfig.topic()).thenReturn(Optional.of("aTopicName"));

        assertTrue(
            new ErrorHandlingStrategy(ErrorHandlingStrategy.FAIL, extensionConfiguration).shouldSendToDlq());
        assertTrue(
            new ErrorHandlingStrategy(ErrorHandlingStrategy.CONTINUE, extensionConfiguration).shouldSendToDlq());
    }
}
