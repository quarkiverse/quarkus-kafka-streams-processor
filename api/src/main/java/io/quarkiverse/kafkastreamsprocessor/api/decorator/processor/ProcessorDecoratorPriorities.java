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
package io.quarkiverse.kafkastreamsprocessor.api.decorator.processor;

import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;

import io.quarkiverse.kafkastreamsprocessor.api.decorator.punctuator.DecoratedPunctuator;

/**
 * Priority levels set on each existing {@link io.quarkiverse.kafkastreamsprocessor.api.Processor} decorators.
 * <p>
 * To be used to define custom decorators:
 * </p>
 *
 * <pre>
 * &#64;Decorator
 * &#64;Priority(150) // to be adapted
 * public class MyDecorator&lt;KIn, VIn, KOut, VOut&gt;
 *     implements Processor&lt;KIn, VIn, KOut, VOut&gt; {
 *   &#64;lombok.experimental.Delegate(excludes = Excludes.class)
 *   private final Processor&lt;KIn, VIn, KOut, VOut&gt; delegate;
 *   &#64;Inject
 *   public MyDecorator(
 *       &#64;Delegate Processor&lt;KIn, VIn, KOut, VOut&gt; delegate) {
 *     this.delegate = delegate;
 *   }
 *   &#64;Override
 *   public void process(Record&lt;KIn, VIn&gt; record) {
 *     context().forward(record);
 *   }
 *   private interface Excludes {
 *     &lt;KIn, VIn&gt; void process(Record&lt;KIn, VIn&gt; record)
 *   }
 * }
 * </pre>
 */
public final class ProcessorDecoratorPriorities {

    /**
     * Priority of the decorator in charge of attaching a vertX context to the running Kafka Streams thread.
     */
    public static final int VERTX_CONTEXT = 50;

    /**
     * Priority of the decorator in charge of tracing, creating a span around the
     * {@link ContextualProcessor#process(Record)} method.
     */
    public static final int TRACING = 100;

    /**
     * Priority of the decorator in charge or initializing a "request context" for the duration of the processing of the
     * ContextualProcessor#process(Record)} method. It is closed afterward.
     */
    public static final int CDI_REQUEST_SCOPE = 200;

    /**
     * Priority for the decorator that wraps the {@link org.apache.kafka.streams.processor.api.ProcessorContext} to
     * intercept calls to its <code>forward</code> methods.
     */
    public static final int CONTEXT_FORWARD = 250;

    /**
     * Priority of the decorator that will handle exception and potentially redirect the message in a dead letter queue
     * topic, if configured.
     */
    public static final int DLQ = 300;

    /**
     * Priority of the decorator in charge of measuring the processing time and the number of exceptions thrown.
     */
    public static final int METRICS = 400;

    /**
     * Priority of the decorator in charge of injecting all {@link DecoratedPunctuator} configured by the framework and
     * your custom potential additions.
     */
    public static final int PUNCTUATOR_DECORATION = 500;

    /**
     * Priority of the decorator in charge of implementing a form of fault tolerance by means of calling again the
     * {@link ContextualProcessor#process(Record)} method.
     */
    public static final int RETRY = 600;

    private ProcessorDecoratorPriorities() {

    }
}
