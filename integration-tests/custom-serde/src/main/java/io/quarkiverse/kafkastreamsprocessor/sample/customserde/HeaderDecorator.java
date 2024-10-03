package io.quarkiverse.kafkastreamsprocessor.sample.customserde;

import java.nio.charset.StandardCharsets;

import jakarta.annotation.Priority;
import jakarta.decorator.Decorator;
import jakarta.decorator.Delegate;
import jakarta.inject.Inject;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;

import io.quarkiverse.kafkastreamsprocessor.api.decorator.processor.ProcessorDecoratorPriorities;

@Decorator
@Priority(ProcessorDecoratorPriorities.PUNCTUATOR_DECORATION + 2)

public class HeaderDecorator<KIn, VIn, KOut, VOut> implements Processor<KIn, VIn, KOut, VOut> {
    @lombok.experimental.Delegate(excludes = Excludes.class)
    private final Processor<KIn, VIn, KOut, VOut> delegate;

    @Inject
    public HeaderDecorator(@Delegate Processor<KIn, VIn, KOut, VOut> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void process(Record<KIn, VIn> record) {
        Header header = record.headers().lastHeader("custom-header");
        if (header != null) {
            String value = new String(header.value(), StandardCharsets.UTF_8);
            if (value.contains("error")) {
                throw new IllegalStateException("Error in header");
            }
        }
        delegate.process(record);
    }

    private interface Excludes {
        <KIn, VIn> void process(Record<KIn, VIn> record);
    }

}
