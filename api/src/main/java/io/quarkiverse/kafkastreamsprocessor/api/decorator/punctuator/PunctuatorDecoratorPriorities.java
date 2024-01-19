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
package io.quarkiverse.kafkastreamsprocessor.api.decorator.punctuator;

import org.apache.kafka.streams.processor.Punctuator;

/**
 * Priorities of all the {@link DecoratedPunctuator} this framework provides. Allows to finely grained the moment your
 * custom decorator will be called.
 */
public final class PunctuatorDecoratorPriorities {
    /**
     * Priority of the {@link DecoratedPunctuator} that enabled a "request context" for the duration of the
     * {@link Punctuator#punctuate(long)} processing.
     */
    public static final int CDI_REQUEST_SCOPE = 100;
    /**
     * Priority of the {@link DecoratedPunctuator} that catches punctuation exception to avoid making the entire
     * microservice crash and counts those exceptions in a metric.
     */
    public static final int METRICS = 200;

    private PunctuatorDecoratorPriorities() {

    }
}
