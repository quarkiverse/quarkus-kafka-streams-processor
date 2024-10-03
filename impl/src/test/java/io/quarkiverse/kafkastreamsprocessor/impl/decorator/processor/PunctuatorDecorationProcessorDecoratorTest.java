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

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;

import jakarta.enterprise.inject.Instance;

import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.quarkiverse.kafkastreamsprocessor.api.decorator.punctuator.DecoratedPunctuator;
import io.quarkiverse.kafkastreamsprocessor.sample.message.PingMessage;

@ExtendWith(MockitoExtension.class)
class PunctuatorDecorationProcessorDecoratorTest {
    @Mock
    Punctuator punctuator;
    @Mock
    DecoratedPunctuator decoratedPunctuator;

    @Mock
    Instance<DecoratedPunctuator> decoratedPunctuators;

    @Mock
    InternalProcessorContext<String, PingMessage.Ping> context;

  @Test
    void contextWrapped() {
        when(decoratedPunctuators.get()).thenReturn(decoratedPunctuator);
        Processor<String, PingMessage.Ping, String, PingMessage.Ping> processor = new Processor<>() {
            @Override
            public void init(ProcessorContext<String, PingMessage.Ping> context) {
                context.schedule(Duration.ofSeconds(10), PunctuationType.WALL_CLOCK_TIME, punctuator);
            }

            @Override
            public void process(Record<String, PingMessage.Ping> record) {

            }
        };

    PunctuatorDecorationProcessorDecorator decorator = new PunctuatorDecorationProcessorDecorator(decoratedPunctuators);
    decorator.setDelegate(processor);
  decorator.init(context);
  decorator.process(new Record<>("blabla",PingMessage.Ping.newBuilder().setMessage("blabla").build(),0L,null));
  decorator.close();

    verify(context).schedule(Duration.ofSeconds(10), PunctuationType.WALL_CLOCK_TIME, decoratedPunctuator);
        verify(decoratedPunctuator).setRealPunctuatorInstance(punctuator);
    }

}
