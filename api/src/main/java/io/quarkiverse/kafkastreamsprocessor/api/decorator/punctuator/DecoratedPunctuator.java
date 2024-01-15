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
 * Interface to implement to be able to decorate Punctuators that would be created.
 * <p>
 * Example:
 * </p>
 *
 * <pre>
 * &#64;Decorator
 * &#64;Priority(150) // to be adapted
 * public class MyDecorator implements DecoratedPunctuator {
 *     &#64;lombok.experimental.Delegate(excludes = Excludes.class)
 *     private final DecoratedPunctuator delegate;
 *
 *     &#64;Inject
 *     public MyDecorator(&#64;Delegate DecoratedPunctuator delegate) {
 *         this.delegate = delegate;
 *     }
 *
 *     &#64;Override
 *     public void punctuate(long timestamp) {
 *         // do something before
 *         delegate.punctuate(timestamp);
 *         // do another thing after
 *     }
 *
 *     private interface Excludes {
 *         void punctuate(long timestamp);
 *     }
 * }
 * </pre>
 */
public interface DecoratedPunctuator extends Punctuator {
    /**
     * Method used by the framework to place the actual {@link Punctuator} in the middle of the different layers of
     * decorators gathered with CDI.
     *
     * @param realPunctuatorInstance
     *        the real instance of punctuator this DecoratedPunctuator decorates
     */
    void setRealPunctuatorInstance(Punctuator realPunctuatorInstance);
}
