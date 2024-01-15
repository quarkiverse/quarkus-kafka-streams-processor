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

import jakarta.annotation.Priority;
import jakarta.decorator.Decorator;
import jakarta.decorator.Delegate;
import jakarta.inject.Inject;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;
import org.eclipse.microprofile.faulttolerance.Retry;

import io.quarkiverse.kafkastreamsprocessor.api.decorator.processor.ProcessorDecoratorPriorities;
import lombok.extern.slf4j.Slf4j;

/**
 * Decorate a {@link Processor#process} with the {@link Retry} fault tolerance annotation.
 */
@Slf4j
@Decorator
@Priority(ProcessorDecoratorPriorities.RETRY)
public class RetryDecorator<KIn, VIn, KOut, VOut> implements Processor<KIn, VIn, KOut, VOut> {
    /**
     * Injection point for composition
     */
    @lombok.experimental.Delegate(excludes = Excludes.class)
    private final Processor<KIn, VIn, KOut, VOut> delegate;

    /**
     * The delegate object that has the processor method with the {@link Retry} annotation.
     * <p>
     * It was mandatory to have a separate class as fault tolerance annotations are not instrumented in a
     * {@link Decorator} annotated class.
     * </p>
     *
     * @see RetryDecoratorDelegate
     */
    private final RetryDecoratorDelegate retryDecoratorDelegate;

    /**
     * Injection constructor
     *
     * @param delegate
     *        injection point for composition
     * @param retryDecoratorDelegate
     *        the separate class with a process method annotated with {@link Retry}
     */
    @Inject
    public RetryDecorator(@Delegate Processor<KIn, VIn, KOut, VOut> delegate,
            RetryDecoratorDelegate retryDecoratorDelegate) {
        this.delegate = delegate;
        this.retryDecoratorDelegate = retryDecoratorDelegate;
    }

    /**
     * Makes use the process method in the { @link RetryDecoratorDelegate} object annotated with {@link Retry}
     * <p>
     * <strong>Original documentation:</strong>
     * </p>
     * {@inheritDoc}
     */
    @Override
    public void process(Record<KIn, VIn> record) {
        try {
            retryDecoratorDelegate.retryableProcess(delegate, record);
        } catch (RuntimeException e) {
            log.info("An exception that has been raised by the processor will not be retried.\n"
                    + "Possible causes:\n"
                    + "- That's not a managed retryable exception\n"
                    + "- maxRetries or maxDuration limits have been reached");
            throw e;
        }
    }

    private interface Excludes {
        <KIn, VIn> void process(Record<KIn, VIn> record);
    }

}
