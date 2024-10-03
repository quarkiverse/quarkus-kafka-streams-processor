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
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;

import io.quarkiverse.kafkastreamsprocessor.api.decorator.processor.AbstractProcessorDecorator;
import io.quarkiverse.kafkastreamsprocessor.api.decorator.processor.ProcessorDecoratorPriorities;
import io.quarkus.arc.Arc;
import io.quarkus.arc.ArcContainer;

/**
 * This class is responsible to manage the lifecycle of {@link jakarta.enterprise.context.RequestScoped} beans. It
 * activates and terminates the request scope upon each processing of message.
 * <p>
 * <strong>Note:</strong> In case where the request context is already active, the Java SDK assumes that the context
 * will be cleaned by the initiator of this activation.
 * <p>
 * <strong>Warning:</strong> "Quarkus Tests" Junit extension is already managing the request scope on its own.
 */
//@Decorator
@Dependent
@Priority(ProcessorDecoratorPriorities.CDI_REQUEST_SCOPE)
//@RequiredArgsConstructor(access = AccessLevel.MODULE)
public class CdiRequestContextDecorator extends AbstractProcessorDecorator {
    /**
     * The container object from Arc to inquire on request contextualization availability and activation
     */
    private final ArcContainer container;

    /**
     * Constructor for injection of the delegate.
     */
    @Inject
    public CdiRequestContextDecorator() {
        this(Arc.container());
    }

    public CdiRequestContextDecorator(ArcContainer container) {
        this.container = container;
    }

    /**
     * If a request contextualization is active, a request context is created before and terminated after the delegation
     * call to {@link Processor#process(Record)}.
     * <p>
     * {@inheritDoc}
     */
    @Override
    public void process(Record record) {
        if (container.requestContext().isActive()) {
            getDelegate().process(record);
        } else {
            container.requestContext().activate();
            try {
                getDelegate().process(record);
            } finally {
                container.requestContext().terminate();
            }
        }
    }
}
