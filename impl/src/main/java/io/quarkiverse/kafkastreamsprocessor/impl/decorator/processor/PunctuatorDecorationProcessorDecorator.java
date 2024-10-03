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

import java.time.Duration;

import jakarta.annotation.Priority;
import jakarta.decorator.Decorator;
import jakarta.decorator.Delegate;
import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;

import io.quarkiverse.kafkastreamsprocessor.api.decorator.processor.AbstractProcessorDecorator;
import io.quarkiverse.kafkastreamsprocessor.api.decorator.processor.ProcessorDecoratorPriorities;
import io.quarkiverse.kafkastreamsprocessor.api.decorator.punctuator.DecoratedPunctuator;
import lombok.RequiredArgsConstructor;

/**
 * {@link Processor} decorator to be able to decorate any {@link Punctuator} that would be injected in the
 * {@link ProcessorContext}.
 *
 * @see PunctuatorDecorationProcessorContextDecorator
 */
//@Decorator
@Priority(ProcessorDecoratorPriorities.PUNCTUATOR_DECORATION)
@Dependent
public class PunctuatorDecorationProcessorDecorator extends AbstractProcessorDecorator {
    /**
     * List of all the {@link Punctuator} decorators defined in this library and potential extensions made with the API.
     */
    private final Instance<DecoratedPunctuator> decoratedPunctuators;

    /**
     * Injection constructor.
     *
     * @param decoratedPunctuators
     *        the list of all {@link Punctuator} decorators defined in this library and potential extensions made with
     *        the API.
     */
    @Inject
    public PunctuatorDecorationProcessorDecorator(
            Instance<DecoratedPunctuator> decoratedPunctuators) {
        this.decoratedPunctuators = decoratedPunctuators;
    }

    /**
     * Injects a {@link ProcessorContext} to be able to decorate any {@link Punctuator} an application would inject.
     * <p>
     * <string>Original documentation:</string>
     * </p>
     * {@inheritDoc}
     *
     * @see PunctuatorDecorationProcessorContextDecorator
     */
    @Override
    public void init(ProcessorContext context) {
        getDelegate().init(new PunctuatorDecorationProcessorContextDecorator<>((InternalProcessorContext) context,
                decoratedPunctuators));
    }

    /**
     * Decorator for ProcessorContext to intercept any {@link Punctuator} configure to decorate them with the
     * {@link DecoratedPunctuator} configured.
     * <p>
     * <strong>Note:</strong> as long as we use org.apache.kafka.streams.processor.internals.ProcessorAdapter, we need to
     * implement InternalProcessorContext due to https://issues.apache.org/jira/browse/KAFKA-9109.
     * </p>
     *
     * @see DecoratedPunctuator
     */
    @RequiredArgsConstructor
    static class PunctuatorDecorationProcessorContextDecorator<KForward, VForward>
            implements InternalProcessorContext<KForward, VForward> {
        @lombok.experimental.Delegate(excludes = Excludes.class)
        private final InternalProcessorContext<KForward, VForward> delegate;

        private final Instance<DecoratedPunctuator> decoratedPunctuators;

        /**
         * Decorates the punctuator instance before forwarding it to the actual {@link InternalProcessorContext}.
         * <p>
         * <b>Original documentation:</b>
         * </p>
         * {@inheritDoc}
         */
        @Override
        public Cancellable schedule(Duration interval, PunctuationType type, Punctuator punctuator) {
            DecoratedPunctuator decoratedPunctuator = decoratedPunctuators.get();
            decoratedPunctuator.setRealPunctuatorInstance(punctuator);
            return delegate.schedule(interval, type, decoratedPunctuator);
        }

        private interface Excludes {
            Cancellable schedule(Duration interval, PunctuationType type, Punctuator callback);
        }
    }

    private interface Excludes {
        <KOut, VOut> void init(ProcessorContext<KOut, VOut> ctx);
    }
}
