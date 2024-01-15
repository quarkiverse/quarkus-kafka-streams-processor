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

import java.time.Duration;

import jakarta.enterprise.context.Dependent;

import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;

import io.quarkiverse.kafkastreamsprocessor.api.decorator.punctuator.DecoratedPunctuator;
import lombok.Setter;
import lombok.experimental.Delegate;

/**
 * Default implementation of a {@link DecoratedPunctuator} which plays the role of final receiver of the actual
 * {@link Punctuator} an application would inject.
 * <p>
 * Indeed, it does compose as others the {@link DecoratedPunctuator#setRealPunctuatorInstance(Punctuator)} method.
 * </p>
 */
@Dependent
public class DefaultDecoratedPunctuator implements DecoratedPunctuator {
    /**
     * Reference to store the actual instances of {@link Punctuator} that an application configures with
     * {@link ProcessorContext#schedule(Duration, PunctuationType, Punctuator)}.
     */
    @Delegate
    @Setter
    private Punctuator realPunctuatorInstance;
}
