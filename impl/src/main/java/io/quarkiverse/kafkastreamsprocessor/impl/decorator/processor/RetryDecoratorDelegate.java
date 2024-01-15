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

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;
import org.eclipse.microprofile.faulttolerance.Retry;

import io.quarkiverse.kafkastreamsprocessor.api.exception.RetryableException;

/**
 * Application scoped bean required so the smallrye fault tolerance annotation {@link Retry} is taken into account.
 * <p>
 * Indeed, the same method in the {@link jakarta.decorator.Decorator}-annotated {@link RetryDecorator} directly is not
 * working.
 * </p>
 */
@ApplicationScoped
public class RetryDecoratorDelegate {
    /**
     * Passthrough method just to wrap {@link Processor#process(Record)} with a {@link Retry} annotation that actually
     * gets parsed and instrumented by the <code>quarkus-smallrye-fault-tolerance</code> extension.
     *
     * @param processor
     *        the actual processor object to pass down the call to {@link Processor#process(Record)}
     * @param record
     *        the argument to {@link Processor#process(Record)}
     * @param <KIn>
     *        the generic key type
     * @param <VIn>
     *        the generic value type
     */
    @Retry(retryOn = RetryableException.class, maxRetries = -1)
    public <KIn, VIn> void retryableProcess(Processor<KIn, VIn, ?, ?> processor, Record<KIn, VIn> record) {
        processor.process(record);
    }
}
