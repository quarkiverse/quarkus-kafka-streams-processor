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

import org.apache.kafka.streams.processor.api.Processor;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Delegate;

/**
 * Base class for all processor decorators.
 * <p>
 * If a decorator does not implement this abstract class, it will not be found by the
 * <code>KafkaClientSuppliedDecorator</code> for composition.
 * </p>
 * <p>
 * We remove the generic declaration from {@link Processor} because ArC complains about generics on class declaration of
 * a bean.
 * </p>
 * <p>
 * Class introduced in 2.0, for compatibility with Quarkus 3.8 random failure to start when using custom processor
 * decorators.
 * </p>
 *
 * @deprecated It will be removed in 3.0, with the integration of Quarkus 3.15 where we will be able to go back to pure
 *             CDI decorators.
 */
@Deprecated(forRemoval = true, since = "2.0")
public abstract class AbstractProcessorDecorator implements Processor {
    /**
     * The decorated processor, holding either the next decorator layer or the final processor.
     */
    @Delegate
    @Getter
    @Setter
    private Processor delegate;
}
