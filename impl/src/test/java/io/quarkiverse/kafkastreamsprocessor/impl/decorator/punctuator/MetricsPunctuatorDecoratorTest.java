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
package io.quarkiverse.kafkastreamsprocessor.impl.decorator.punctuator;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

import java.time.Instant;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.quarkiverse.kafkastreamsprocessor.api.decorator.punctuator.DecoratedPunctuator;
import io.quarkiverse.kafkastreamsprocessor.impl.TestException;
import io.quarkiverse.kafkastreamsprocessor.impl.metrics.MockKafkaStreamsProcessorMetrics;

@ExtendWith(MockitoExtension.class)
class MetricsPunctuatorDecoratorTest {
    @Mock
    DecoratedPunctuator punctuator;

    MetricsPunctuatorDecorator decorator;

    MockKafkaStreamsProcessorMetrics metrics = new MockKafkaStreamsProcessorMetrics();

    @BeforeEach
    void setUp() {
        decorator = new MetricsPunctuatorDecorator(punctuator, metrics);
    }

    @Test
    void noError() {
        long timestamp = Instant.now().getEpochSecond();

        decorator.punctuate(timestamp);

        verify(punctuator).punctuate(timestamp);
        assertThat(metrics.punctuatorErrorCounter().count(), closeTo(0d, 0.01d));
    }

    @Test
    void exceptionThrownAndCaught() {
        long timestamp = Instant.now().getEpochSecond();
        doThrow(new TestException()).when(punctuator).punctuate(timestamp);

        decorator.punctuate(timestamp);

        assertThat(metrics.punctuatorErrorCounter().count(), closeTo(1d, 0.01d));
    }
}
