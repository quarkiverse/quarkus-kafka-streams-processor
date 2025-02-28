package io.quarkiverse.kafkastreamsprocessor.impl.decorator.processor;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import jakarta.enterprise.inject.Instance;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.processor.api.InternalFixedKeyRecordFactory;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;

import io.quarkiverse.kafkastreamsprocessor.api.decorator.outputrecord.OutputRecordInterceptor;
import lombok.RequiredArgsConstructor;

@ExtendWith(MockitoExtension.class)
class OutputRecordInterceptionDecoratorTest {
    @Mock
    OutputRecordInterceptor outputRecordInterceptor1;

    @Mock
    OutputRecordInterceptor outputRecordInterceptor2;

    @Mock
    Instance<OutputRecordInterceptor> forwardDecorators;

    @Mock
    Processor<String, String, String, String> delegate;

    @Mock
    InternalProcessorContext<String, String> context;

    OutputRecordInterceptionDecorator decorator;

    Headers header1;

    @Captor
    ArgumentCaptor<Record<String, String>> recordCaptor;

    @Captor
    ArgumentCaptor<String> childCaptor;

    @BeforeEach
    void setUp() {
        when(forwardDecorators.stream())
                .thenReturn(List.of((OutputRecordInterceptor) outputRecordInterceptor1,
                        (OutputRecordInterceptor) outputRecordInterceptor2).stream());
        decorator = new OutputRecordInterceptionDecorator(forwardDecorators);
        header1 = new RecordHeaders().add("header1", "value1".getBytes(StandardCharsets.UTF_8));
        Headers header2 = new RecordHeaders().add("header2", "value2".getBytes(StandardCharsets.UTF_8));
        Headers header3 = new RecordHeaders().add("header3", "value3".getBytes(StandardCharsets.UTF_8));
        doAnswer(new ForwardAnswer("aKey", "aValue", header1,
                new Record("anotherKey", "anotherValue", 0L, header2)))
                .when(outputRecordInterceptor1).interceptOutputRecord(any());
        doAnswer(new ForwardAnswer("anotherKey", "anotherValue", header2,
                new Record("yetAnotherKey", "yetAnotherValue", 0L, header3)))
                .when(outputRecordInterceptor2).interceptOutputRecord(any());
    }

    @RequiredArgsConstructor
    private static class ForwardAnswer implements Answer<Record> {
        private final String key;
        private final String value;
        private final Headers headers;
        private final Record result;

        @Override
        public Record answer(InvocationOnMock invocation) {
            if (invocation.getArguments().length == 1) {
                Record actualRecord = invocation.getArgument(0);
                Map<String, List<String>> actualHeaders = headersToMap(actualRecord.headers());
                Map<String, List<String>> expectedHeaders = headersToMap(headers);
                if (key.equals(actualRecord.key()) && value.equals(actualRecord.value())
                        && expectedHeaders.equals(actualHeaders)) {
                    return result;
                }
                throw new AssertionError("Unexpected arguments:\nexpectedKey=" + key + " actualKey=" + actualRecord.key()
                        + "\nexpectedValue=" + value + " actualValue=" + actualRecord.value() + "\nexpectedHeaders="
                        + expectedHeaders
                        + " actualHeaders=" + actualHeaders);
            }
            return null;
        }

        private static Map<String, List<String>> headersToMap(Headers headers) {
            return StreamSupport.stream(headers.spliterator(), false)
                    .collect(Collectors.groupingBy(Header::key,
                            Collectors.mapping(h -> new String(h.value(), StandardCharsets.UTF_8), Collectors.toList())));
        }
    }

    @Test
    public void keyValueForwarding() {
        decorator.setDelegate(new KeyValueForwardingProcessor());
        when(context.timestamp()).thenReturn(1023L);
        when(context.headers()).thenReturn(header1);

        decorator.init(context);
        Record<String, String> record = new Record<String, String>("aKey", "aValue", 0L, header1);
        decorator.process(record);

        verify(context).forward(recordCaptor.capture(), childCaptor.capture());

        Record<String, String> forwardedRecord = recordCaptor.getValue();
        String child = childCaptor.getValue();

        assertThat(forwardedRecord.key(), equalTo("yetAnotherKey"));
        assertThat(forwardedRecord.value(), equalTo("yetAnotherValue"));
        assertThat(new String(forwardedRecord.headers().lastHeader("header3").value(), StandardCharsets.UTF_8),
                equalTo("value3"));
        assertThat(child, nullValue());
    }

    @Test
    public void keyValueForwardingTo() {
        decorator.setDelegate(new KeyValueToForwardingProcessor());
        when(context.timestamp()).thenReturn(1023L);
        when(context.headers()).thenReturn(header1);

        decorator.init(context);
        Record<String, String> record = new Record<String, String>("aKey", "aValue", 0L, header1);
        decorator.process(record);

        verify(context).forward(recordCaptor.capture(), childCaptor.capture());

        Record<String, String> forwardedRecord = recordCaptor.getValue();
        String child = childCaptor.getValue();

        assertThat(forwardedRecord.key(), equalTo("yetAnotherKey"));
        assertThat(forwardedRecord.value(), equalTo("yetAnotherValue"));
        assertThat(new String(forwardedRecord.headers().lastHeader("header3").value(), StandardCharsets.UTF_8),
                equalTo("value3"));
        assertThat(child, equalTo("child"));
    }

    @Test
    public void fixedKeyRecord() {
        decorator.setDelegate(new FixedKeyRecordForwardingProcessor());

        decorator.init(context);
        Record<String, String> record = new Record<String, String>("aKey", "aValue", 0L, header1);
        decorator.process(record);

        verify(context).forward(recordCaptor.capture(), childCaptor.capture());

        Record<String, String> forwardedRecord = recordCaptor.getValue();
        String child = childCaptor.getValue();

        assertThat(forwardedRecord.key(), equalTo("yetAnotherKey"));
        assertThat(forwardedRecord.value(), equalTo("yetAnotherValue"));
        assertThat(new String(forwardedRecord.headers().lastHeader("header3").value(), StandardCharsets.UTF_8),
                equalTo("value3"));
        assertThat(child, nullValue());
    }

    @Test
    public void fixedKeyRecordTo() {
        decorator.setDelegate(new FixedKeyRecordToForwardingProcessor());

        decorator.init(context);
        Record<String, String> record = new Record<String, String>("aKey", "aValue", 0L, header1);
        decorator.process(record);

        verify(context).forward(recordCaptor.capture(), childCaptor.capture());

        Record<String, String> forwardedRecord = recordCaptor.getValue();
        String child = childCaptor.getValue();

        assertThat(forwardedRecord.key(), equalTo("yetAnotherKey"));
        assertThat(forwardedRecord.value(), equalTo("yetAnotherValue"));
        assertThat(new String(forwardedRecord.headers().lastHeader("header3").value(), StandardCharsets.UTF_8),
                equalTo("value3"));
        assertThat(child, equalTo("child"));
    }

    @Test
    public void record() {
        decorator.setDelegate(new RecordForwardingProcessor());

        decorator.init(context);
        Record<String, String> record = new Record<String, String>("aKey", "aValue", 0L, header1);
        decorator.process(record);

        verify(context).forward(recordCaptor.capture(), childCaptor.capture());

        Record<String, String> forwardedRecord = recordCaptor.getValue();
        String child = childCaptor.getValue();

        assertThat(forwardedRecord.key(), equalTo("yetAnotherKey"));
        assertThat(forwardedRecord.value(), equalTo("yetAnotherValue"));
        assertThat(new String(forwardedRecord.headers().lastHeader("header3").value(), StandardCharsets.UTF_8),
                equalTo("value3"));
        assertThat(child, nullValue());
    }

    @Test
    public void recordTo() {
        decorator.setDelegate(new RecordToForwardingProcessor());

        decorator.init(context);
        Record<String, String> record = new Record<String, String>("aKey", "aValue", 0L, header1);
        decorator.process(record);

        verify(context).forward(recordCaptor.capture(), childCaptor.capture());

        Record<String, String> forwardedRecord = recordCaptor.getValue();
        String child = childCaptor.getValue();

        assertThat(forwardedRecord.key(), equalTo("yetAnotherKey"));
        assertThat(forwardedRecord.value(), equalTo("yetAnotherValue"));
        assertThat(new String(forwardedRecord.headers().lastHeader("header3").value(), StandardCharsets.UTF_8),
                equalTo("value3"));
        assertThat(child, equalTo("child"));
    }

    private static abstract class InternalContextualProcessor implements Processor<String, String, String, String> {
        InternalProcessorContext<String, String> context;

        @Override
        public void init(ProcessorContext<String, String> context) {
            Processor.super.init(context);
            this.context = (InternalProcessorContext<String, String>) context;
        }
    }

    private static final class KeyValueForwardingProcessor extends InternalContextualProcessor {
        @Override
        public void process(Record<String, String> record) {
            context.forward(record.key(), record.value());
        }
    }

    private static final class KeyValueToForwardingProcessor extends InternalContextualProcessor {
        @Override
        public void process(Record<String, String> record) {
            context.forward(record.key(), record.value(), To.child("child"));
        }
    }

    private static final class FixedKeyRecordForwardingProcessor extends InternalContextualProcessor {
        @Override
        public void process(Record<String, String> record) {
            FixedKeyRecord<String, String> fixedKeyRecord = InternalFixedKeyRecordFactory.create(record);
            context.forward(fixedKeyRecord);
        }
    }

    private static final class FixedKeyRecordToForwardingProcessor extends InternalContextualProcessor {
        @Override
        public void process(Record<String, String> record) {
            FixedKeyRecord<String, String> fixedKeyRecord = InternalFixedKeyRecordFactory.create(record);
            context.forward(fixedKeyRecord, "child");
        }
    }

    private static final class RecordForwardingProcessor extends InternalContextualProcessor {
        @Override
        public void process(Record<String, String> record) {
            context.forward(record);
        }
    }

    private static final class RecordToForwardingProcessor extends InternalContextualProcessor {
        @Override
        public void process(Record<String, String> record) {
            context.forward(record, "child");
        }
    }

}
