package io.quarkiverse.kafkastreamsprocessor.impl.decorator.processor;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;

import org.apache.kafka.streams.processor.api.Record;

import io.quarkiverse.kafkastreamsprocessor.api.decorator.processor.AbstractProcessorDecorator;
import io.quarkiverse.kafkastreamsprocessor.api.decorator.processor.ProcessorDecoratorPriorities;
import io.smallrye.common.vertx.VertxContext;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.impl.ContextInternal;
import lombok.extern.slf4j.Slf4j;

/**
 * Processor decorator to attach a Vert.x duplicated context to the Kafka Stream managed thread
 * <p>
 * This is typically required for using ContextLocals API to share metadata between different pieces of code.
 */
@Slf4j
@Dependent
@Priority(ProcessorDecoratorPriorities.VERTX_CONTEXT)
public class VertxContextDecorator extends AbstractProcessorDecorator {

    @Inject
    Vertx vertx;

    @Override
    public void process(Record record) {
        Context vertxContext = vertx.getOrCreateContext();
        Context duplicatedContext = VertxContext.createNewDuplicatedContext(vertxContext);

        ContextInternal duplicatedContextInternal = (ContextInternal) duplicatedContext;
        duplicatedContextInternal.dispatch(event -> getDelegate().process(record));
    }
}
