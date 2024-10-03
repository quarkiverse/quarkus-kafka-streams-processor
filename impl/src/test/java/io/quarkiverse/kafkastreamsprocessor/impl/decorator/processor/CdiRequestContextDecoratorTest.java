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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.quarkiverse.kafkastreamsprocessor.impl.TestException;
import io.quarkus.arc.ArcContainer;
import io.quarkus.arc.ManagedContext;

@ExtendWith(MockitoExtension.class)
class CdiRequestContextDecoratorTest {

    CdiRequestContextDecorator decorator;

    @Mock
    ArcContainer container;

    @Mock
    Processor<String, String, String, String> processor;

    @Mock
    ManagedContext requestContext;

  @BeforeEach
  public void setup() {
    when(container.requestContext()).thenReturn(requestContext);
    decorator = new CdiRequestContextDecorator(container);
    decorator.setDelegate(processor);
  }

    @Test
    public void shouldActivateDeactivateWhenProcessIsCalled() {
        doThrow(TestException.class).when(processor).process(any());
        assertThrows(TestException.class, () -> decorator.process(new Record<>("Hello", "World", 0L)));
        verify(requestContext).activate();
        verify(requestContext).terminate();
    }

  @Test
  public void shouldNotActivateRequestScopeIfAlreadyActivated() {
    when(requestContext.isActive()).thenReturn(true);
    decorator.process(new Record<>("Hello", "World", 0L));
    verify(requestContext, never()).activate();
  }
}
