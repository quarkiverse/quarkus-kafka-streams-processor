package io.quarkiverse.kafkastreamsprocessor.impl.decorator.processor;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorNode;
import org.apache.kafka.streams.processor.internals.ToInternal;

import io.quarkiverse.kafkastreamsprocessor.api.decorator.outputrecord.OutputRecordInterceptor;
import io.quarkiverse.kafkastreamsprocessor.api.decorator.processor.AbstractProcessorDecorator;
import io.quarkiverse.kafkastreamsprocessor.api.decorator.processor.ProcessorDecoratorPriorities;
import lombok.RequiredArgsConstructor;

// @Decorator
@Dependent
@Priority(ProcessorDecoratorPriorities.CONTEXT_FORWARD)
// @RequiredArgsConstructor(access = AccessLevel.MODULE)
public class OutputRecordInterceptionDecorator extends AbstractProcessorDecorator {
    private final List<OutputRecordInterceptor> outputRecordInterceptors;

    @Inject
    public OutputRecordInterceptionDecorator(Instance<OutputRecordInterceptor> forwardDecorators) {
        this.outputRecordInterceptors = forwardDecorators.stream().collect(Collectors.toList());
        Collections.sort(this.outputRecordInterceptors, Comparator.comparingInt(OutputRecordInterceptor::priority));
    }

    @Override
    public void init(ProcessorContext context) {
        getDelegate().init(new ContextForwardProcessorContextDecorator((InternalProcessorContext) context,
                outputRecordInterceptors));
    }

    @RequiredArgsConstructor
    public static class ContextForwardProcessorContextDecorator
            implements InternalProcessorContext {
        @lombok.experimental.Delegate(excludes = Excludes.class)
        private final InternalProcessorContext delegate;

        private final List<OutputRecordInterceptor> outputRecordInterceptors;

        @Override
        public <K, V> void forward(K key, V value) {
            Record<K, V> toForward = new Record(key, value, timestamp(), headers());
            forward(toForward);
        }

        @Override
        public <K, V> void forward(final K key, final V value, final To to) {
            ToInternal toInternal = new ToInternal(to);
            Record<K, V> toForward = new Record(key, value,
                    toInternal.hasTimestamp() ? toInternal.timestamp() : timestamp(), headers());
            forward(toForward, toInternal.child());
        }

        @Override
        public void forward(FixedKeyRecord record) {
            forward(new Record(record.key(), record.value(), record.timestamp(), record.headers()));
        }

        @Override
        public void forward(FixedKeyRecord record, String childName) {
            forward(new Record(record.key(), record.value(), record.timestamp(), record.headers()), childName);
        }

        @Override
        public void forward(Record record) {
            forward(record, null);
        }

        @Override
        public void forward(Record record, String childName) {
            for (OutputRecordInterceptor outputRecordInterceptor : outputRecordInterceptors) {
                record = outputRecordInterceptor.interceptOutputRecord(record);
            }
            delegate.forward(record, childName);
        }

        // Bug of @Delegate from lombok, not able to handle the generics of ProcessorNode
        @Override
        public void setCurrentNode(ProcessorNode currentNode) {
            delegate.setCurrentNode(currentNode);
        }

        private interface Excludes {
            <K, V> void forward(K var1, V var2);

            <K, V> void forward(K var1, V var2, To var3);

            void forward(FixedKeyRecord fixedKeyRecord);

            void forward(FixedKeyRecord fixedKeyRecord, String s);

            void forward(Record record);

            void forward(Record record, String s);

            void setCurrentNode(ProcessorNode node);
        }
    }
}
