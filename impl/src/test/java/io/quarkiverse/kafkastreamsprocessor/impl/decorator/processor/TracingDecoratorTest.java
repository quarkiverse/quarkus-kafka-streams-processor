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

import static io.opentelemetry.sdk.testing.assertj.TracesAssert.assertThat;
import static io.quarkiverse.kafkastreamsprocessor.impl.protocol.KafkaStreamsProcessorHeaders.W3C_BAGGAGE;
import static io.quarkiverse.kafkastreamsprocessor.impl.protocol.KafkaStreamsProcessorHeaders.W3C_TRACE_ID;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;
import java.util.logging.Formatter;
import java.util.logging.LogManager;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.hamcrest.MatcherAssert;
import org.jboss.logmanager.Level;
import org.jboss.logmanager.formatters.PatternFormatter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.MDC;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.baggage.Baggage;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanId;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceId;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.sdk.trace.IdGenerator;
import io.quarkiverse.kafkastreamsprocessor.impl.TestException;
import io.quarkiverse.kafkastreamsprocessor.impl.configuration.TopologyConfigurationImpl;
import io.quarkiverse.kafkastreamsprocessor.impl.utils.OpenTelemetryWithBaggageExtension;
import io.quarkiverse.kafkastreamsprocessor.propagation.KafkaTextMapGetter;
import io.quarkiverse.kafkastreamsprocessor.propagation.KafkaTextMapSetter;
import io.quarkiverse.kafkastreamsprocessor.sample.message.PingMessage.Ping;
import io.quarkus.test.InMemoryLogHandler;
import io.quarkus.vertx.core.runtime.VertxMDC;
import lombok.extern.slf4j.Slf4j;

/**
 * Note: launch the test via Maven to actually see the MDC in log lines.
 */
@ExtendWith({ MockitoExtension.class })
public class TracingDecoratorTest {
    // header value format here: https://www.w3.org/TR/trace-context/#traceparent-header
    private static final String TRACE_PARENT = w3cHeader("1", "9");
    private static final String PROCESSOR_NAME = MockType.class.getName();
    private static final Formatter LOG_FORMATTER = new PatternFormatter("%p %s %e");
    private static final InMemoryLogHandler inMemoryLogHandler = new InMemoryLogHandler(record -> true);
    private static final java.util.logging.Logger rootLogger = LogManager.getLogManager().getLogger("io.quarkiverse");

    @RegisterExtension
    static final OpenTelemetryWithBaggageExtension otel = OpenTelemetryWithBaggageExtension.create();

    TracingDecorator decorator;

    @Mock
    JsonFormat.Printer jsonPrinter;

    @Spy
    ReadMDCProcessor kafkaProcessor;

    @Mock
    InternalProcessorContext<String, Ping> processorContext;

    @Mock
    TopologyConfigurationImpl topologyConfiguration;

    Tracer tracer = otel.getOpenTelemetry().getTracer("test");

    KafkaTextMapGetter kafkaTextMapGetter = new KafkaTextMapGetter();
    KafkaTextMapSetter kafkaTextMapSetter = new KafkaTextMapSetter();
    Ping inputMessage = Ping.newBuilder().setMessage("message").build();

    @BeforeEach
    public void setUp() {
        inMemoryLogHandler.getRecords().clear();
        rootLogger.addHandler(inMemoryLogHandler);
        rootLogger.setLevel(Level.DEBUG);
        when(topologyConfiguration.getProcessorPayloadType()).thenReturn((Class) MockType.class);
        decorator = new TracingDecorator(otel.getOpenTelemetry(), kafkaTextMapGetter,
                tracer, topologyConfiguration.getProcessorPayloadType().getName(), jsonPrinter);
        decorator.setDelegate(kafkaProcessor);
        decorator.init(processorContext);
    }

    @Test
    public void shouldSetMDCFromUberTraceId() {
        Span parentSpan = tracer.spanBuilder("parent").startSpan();
        try (Scope parentScope = parentSpan.makeCurrent()) {
            Headers headers = new RecordHeaders();
            otel.getOpenTelemetry()
                    .getPropagators()
                    .getTextMapPropagator()
                    .inject(Context.current(), headers, kafkaTextMapSetter);
            Record<String, Ping> record = new Record<>(null, null, 0L, headers);

            decorator.process(record);
        } finally {
            parentSpan.end();
        }

        MatcherAssert.assertThat(kafkaProcessor.traceId, equalTo(parentSpan.getSpanContext().getTraceId()));
        // This is not a QuarkusTest so Quarkus vertx extension has not plugged VertxMDC to jboss-logmanager
        MatcherAssert.assertThat(VertxMDC.INSTANCE.get("traceId"), nullValue());
    }

    @Test
    public void shouldStartAndFinishSpan() {
        // manually build parent span to inject some TraceState and test the state is well recorded in the created span
        Span parentSpan = Span.wrap(SpanContext.create(IdGenerator.random().generateTraceId(), IdGenerator.random()
                .generateSpanId(), TraceFlags.getSampled(), TraceState.builder().put("state1", "value2").build()));
        try (Scope parentScope = parentSpan.makeCurrent()) {
            RecordHeaders headers = new RecordHeaders();
            otel.getOpenTelemetry()
                    .getPropagators()
                    .getTextMapPropagator()
                    .inject(Context.current(), headers, kafkaTextMapSetter);
            Record<String, Ping> record = new Record<>(null, null, 0L, headers);

            decorator.process(record);
        } finally {
            parentSpan.end();
        }

        assertThat(otel.getSpans())
                .hasTracesSatisfyingExactly(
                        trace -> trace.hasSpansSatisfyingExactly(
                                span -> span.hasTraceId(parentSpan.getSpanContext().getTraceId())
                                        .hasName(PROCESSOR_NAME)
                                        .hasParentSpanId(parentSpan.getSpanContext().getSpanId())
                                        .hasTraceState(TraceState.builder().put("state1", "value2").build())));
    }

    @Test
    public void shouldCleanMDCAndScopeInCaseOfException() {
        Span parentSpan = tracer.spanBuilder("parent").startSpan();
        try (Scope parentScope = parentSpan.makeCurrent()) {
            Headers headers = new RecordHeaders();
            otel.getOpenTelemetry()
                    .getPropagators()
                    .getTextMapPropagator()
                    .inject(Context.current(), headers, kafkaTextMapSetter);
            Record<String, Ping> record = new Record<>(null, Ping.newBuilder()
                    .setMessage("blabla")
                    .build(), 0L, headers);

            decorator = new TracingDecorator(otel.getOpenTelemetry(), kafkaTextMapGetter,
                    tracer, topologyConfiguration.getProcessorPayloadType().getName(), jsonPrinter);
            decorator.setDelegate(new ThrowExceptionProcessor());
            decorator.init(processorContext);

            assertDoesNotThrow(() -> decorator.process(record));
        } finally {
            parentSpan.end();
        }

        assertNull(MDC.get("traceId"));

        assertThat(otel.getSpans())
                .hasTracesSatisfyingExactly(trace -> trace.hasSpansSatisfyingExactly(
                        span -> span.hasSpanId(parentSpan.getSpanContext().getSpanId()),
                        span -> span.hasTraceId(parentSpan.getSpanContext().getTraceId())
                                .hasName(PROCESSOR_NAME)
                                .hasStatusSatisfying(status -> status.hasCode(StatusCode.ERROR))
                                .hasException(new TestException())));
    }

    @Test
    public void shouldDelegateAndCreateSpanByDefault() {
        Headers headers = new RecordHeaders();
        Record<String, Ping> record = new Record<>(null, null, 0L, headers);

        decorator.process(record);

        verify(kafkaProcessor).process(record);
        MatcherAssert.assertThat(otel.getSpans(), hasSize(1));
        assertNotNull(kafkaProcessor.traceId);
        // This is not a QuarkusTest so Quarkus vertx extension has not plugged VertxMDC to jboss-logmanager
        assertNull(VertxMDC.INSTANCE.get("traceId"));
    }

    @Test
    void shouldDelegateAllMethodCalls() {
        Record<String, Ping> record = new Record<>("key", inputMessage, 0L);
        decorator.process(record);
        decorator.close();

        verify(kafkaProcessor).init(same(processorContext));
        verify(kafkaProcessor).process(eq(record));
        verify(kafkaProcessor).close();
    }

    @Test
    void shouldManageRuntimeException() throws Throwable {
        RuntimeException exception = new TestException();
        doThrow(exception).when(kafkaProcessor).process(any());
        when(jsonPrinter.print(any())).thenReturn("marshalled");

        decorator.process(new Record<>("key", inputMessage, 0L));

        assertThat(getLogs(), hasItem(allOf(containsString("ERROR"),
                containsString("Runtime error caught while processing the message"), containsString(exception.getMessage()))));
        assertThat(getLogs(), hasItem(allOf(containsString("DEBUG"), containsString("marshalled"))));
    }

    private static List<String> getLogs() {
        return inMemoryLogHandler.getRecords().stream().map(LOG_FORMATTER::format).collect(Collectors.toList());
    }

    @Test
    void shouldLetBubbleUpKafkaExceptionAndLogMessage() {
        doThrow(new KafkaException()).when(kafkaProcessor).process(any());
        Assertions.assertThrows(KafkaException.class,
                () -> decorator.process(new Record<>("key", inputMessage, 0L)));
    }

    @Test
    void shouldExtractMetadataFromProcessorContext() throws Throwable {
        RecordMetadata metadata = mock(RecordMetadata.class);
        when(processorContext.recordMetadata()).thenReturn(Optional.of(metadata));
        when(metadata.partition()).thenReturn(45);
        when(metadata.topic()).thenReturn("blabla");
        RuntimeException exception = new TestException();
        doThrow(exception).when(kafkaProcessor).process(any());

        decorator.process(new Record<>("key", inputMessage, 0L));

        assertThat(getLogs(), hasItem(allOf(containsString("DEBUG"), containsString("topic=blabla, partition=45"))));
    }

    @Test
    void shouldLogMetadataEvenIfValueMarshallingToJSONFails() throws Throwable {
        RuntimeException exception = new TestException();
        doThrow(exception).when(kafkaProcessor).process(any());
        InvalidProtocolBufferException protocolBufferException = new InvalidProtocolBufferException("proto error");
        when(jsonPrinter.print(any())).thenThrow(protocolBufferException);

        decorator.process(new Record<>("key", inputMessage, 0L));

        assertThat(getLogs(),
                hasItem(allOf(containsString("ERROR"), containsString(protocolBufferException.getMessage()))));
        assertThat(getLogs(), hasItem(allOf(containsString("DEBUG"), containsString("value=null"))));
    }

    @Test
    void shouldLogRawToStringValueIfNotProtobuf() throws Throwable {
        Processor<String, String, String, String> kafkaProcessor = mock(Processor.class);
        ProcessorContext<String, String> processorContext = mock(ProcessorContext.class);
        TracingDecorator decorator = new TracingDecorator(GlobalOpenTelemetry.get(), kafkaTextMapGetter,
                tracer, topologyConfiguration.getProcessorPayloadType().getName(), jsonPrinter);
        decorator.setDelegate(kafkaProcessor);
        decorator.init(processorContext);

        RuntimeException exception = new TestException();
        doThrow(exception).when(kafkaProcessor).process(any());

        decorator.process(new Record<>("key", "value", 0));

        assertThat(getLogs(), hasItem(allOf(containsString("DEBUG"), containsString("value=value"))));
        assertThat(getLogs(), hasItem(allOf(containsString("DEBUG"), containsString("time=1970-01-01T00:00Z"))));
    }

    @Test
    void shouldPropagateOpentelemetryW3CBaggage() {
        // header value format here: https://www.w3.org/TR/baggage/#baggage-http-header-format
        Headers headers = new RecordHeaders().add(W3C_TRACE_ID, TRACE_PARENT.getBytes())
                .add(W3C_BAGGAGE, "key1=value1,key2=value2".getBytes());
        Record<String, Ping> record = new Record<>(null, Ping.newBuilder().setMessage("blabla").build(), 0L, headers);
        decorator = new TracingDecorator(otel.getOpenTelemetry(), kafkaTextMapGetter,
                tracer, topologyConfiguration.getProcessorPayloadType().getName(), jsonPrinter);
        decorator.setDelegate(new LogOpentelemetryBaggageProcessor());
        decorator.init(processorContext);

        decorator.process(record);

        assertThat(getLogs(), hasItem(allOf(containsString("DEBUG"), containsString("baggage: key1 value1"))));
        assertThat(getLogs(), hasItem(allOf(containsString("DEBUG"), containsString("baggage: key2 value2"))));
    }

    @Slf4j
    static class ReadMDCProcessor implements Processor<String, Ping, String, Ping> {
        String traceId;

        @Override
        public void process(Record<String, Ping> record) {
            // This is not a QuarkusTest so Quarkus vertx extension has not plugged VertxMDC to jboss-logmanager
            traceId = VertxMDC.INSTANCE.get("traceId");
        }
    }

    static class ThrowExceptionProcessor implements Processor<String, Ping, String, Ping> {
        @Override
        public void process(Record<String, Ping> record) {
            throw new TestException();
        }
    }

    @Slf4j
    static class LogOpentelemetryBaggageProcessor implements Processor<String, Ping, String, Ping> {
        @Override
        public void process(Record<String, Ping> record) {
            Baggage.current().forEach((key, baggageEntry) -> log.debug("baggage: {} {}", key, baggageEntry.getValue()));
        }
    }

    public static String w3cHeader(String traceId, String spanId) {
        return String.format("00-%s-%s-01", StringUtils.leftPad(traceId, TraceId.getLength(), '0'),
                StringUtils.leftPad(spanId, SpanId.getLength(), '0'));
    }

    public static class MockType {

    }
}
