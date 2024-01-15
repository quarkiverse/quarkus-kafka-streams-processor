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

import jakarta.annotation.Priority;
import jakarta.decorator.Decorator;
import jakarta.decorator.Delegate;
import jakarta.inject.Inject;

import io.quarkiverse.kafkastreamsprocessor.api.decorator.punctuator.DecoratedPunctuator;
import io.quarkiverse.kafkastreamsprocessor.api.decorator.punctuator.PunctuatorDecoratorPriorities;
import io.quarkus.arc.Arc;
import io.quarkus.arc.ArcContainer;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

/**
 * Decorator that activates a request context for the execution of the punctuator.
 * <p>
 * It is more a utility decoration to prevent the failure of injection and execution of any bean/method that requires a
 * request context.
 * </p>
 */
@Decorator
@Priority(PunctuatorDecoratorPriorities.CDI_REQUEST_SCOPE)
@RequiredArgsConstructor(access = AccessLevel.MODULE)
public class CdiRequestContextPunctuatorDecorator implements DecoratedPunctuator {
    /**
     * Injection point for composition
     */
    @lombok.experimental.Delegate(excludes = Excludes.class)
    private final DecoratedPunctuator delegate;

    /**
     * The container of the Arc library of Quarkus
     * <p>
     * Avoids a call to static methods {@link Arc#container()} for the unit tests.
     * </p>
     */
    private final ArcContainer container;

    /**
     * Injection constructor
     *
     * @param delegate
     *        injection point for composition
     */
    @Inject
    public CdiRequestContextPunctuatorDecorator(@Delegate DecoratedPunctuator delegate) {
        this(delegate, Arc.container());
    }

    /**
     * Decorate punctuation to activate before and terminate after a "request scope" in Arc if possible.
     * <p>
     * <strong>Documentation of the decorated method:</strong>
     * </p>
     * {@inheritDoc}
     */
    @Override
    public void punctuate(long timestamp) {
        if (container.requestContext().isActive()) {
            delegate.punctuate(timestamp);
        } else {
            container.requestContext().activate();
            try {
                delegate.punctuate(timestamp);
            } finally {
                container.requestContext().terminate();
            }
        }
    }

    private interface Excludes {
        void punctuate(long timestamp);
    }
}
