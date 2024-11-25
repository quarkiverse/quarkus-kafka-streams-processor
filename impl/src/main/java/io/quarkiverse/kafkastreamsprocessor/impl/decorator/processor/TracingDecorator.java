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

import static java.util.stream.StreamSupport.stream;

import java.nio.charset.Charset;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import jakarta.annotation.Priority;
import jakarta.decorator.Decorator;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.quarkiverse.kafkastreamsprocessor.api.decorator.processor.AbstractProcessorDecorator;
import io.quarkiverse.kafkastreamsprocessor.api.decorator.processor.ProcessorDecoratorPriorities;
import io.quarkiverse.kafkastreamsprocessor.impl.configuration.TopologyConfigurationImpl;
import io.quarkiverse.kafkastreamsprocessor.impl.protocol.KafkaStreamsProcessorHeaders;
import io.quarkiverse.kafkastreamsprocessor.propagation.KafkaTextMapGetter;
import lombok.extern.slf4j.Slf4j;

/**
 * Decorator of {@link Processor} that creates a follow-from span around processors and catches exceptions to log them.
 * It would make KafkaStreams shutdown and the probes start failing.
 * <p>
 * For the tracing part: to be replaced by a proper Kafka consumer/producer instrumentation. Slf4j MDC has to be set on
 * the thread that processes messages for a given topic-partition. This excludes using
 * {@link org.apache.kafka.clients.consumer.ConsumerInterceptor}, which executes on the polling thread.
 */
@Slf4j
//@Decorator
@Priority(ProcessorDecoratorPriorities.TRACING)
@Dependent
public class TracingDecorator extends AbstractProcessorDecorator {
    /**
     * The {@link OpenTelemetry} configured by Quarkus
     */
    private final OpenTelemetry openTelemetry;

    /**
     * Extracts Context from the Kafka headers of a message
     */
    private final KafkaTextMapGetter textMapGetter;

    /**
     * The tracer instance to create spans
     */
    private final Tracer tracer;

    /**
     * Application name to add as metadata on spans
     */
    private final String applicationName;

    /**
     * Protobuf printer to log the message in case of unsolvable Exception.
     */
    private final JsonFormat.Printer jsonPrinter;

    /**
     * Reference to {@link ProcessorContext} cause we can't extend {@link ContextualProcessor} for decoration to work.
     */
    private ProcessorContext context;

    /**
     * Injection constructor.
     *
     * @param openTelemetry
     *        The {@link OpenTelemetry} configured by Quarkus
     * @param textMapGetter
     *        Extracts Context from the Kafka headers of a message
     * @param tracer
     *        The tracer instance to create spans
     * @param configuration
     *        The TopologyConfiguration after customization.
     */
    @Inject
    public TracingDecorator(OpenTelemetry openTelemetry, KafkaTextMapGetter textMapGetter, Tracer tracer,
            TopologyConfigurationImpl configuration) {
        this(openTelemetry, textMapGetter, tracer, configuration.getProcessorPayloadType().getName(),
                JsonFormat.printer());
    }

    public TracingDecorator(OpenTelemetry openTelemetry, KafkaTextMapGetter textMapGetter,
            Tracer tracer, String applicationName, JsonFormat.Printer jsonPrinter) {
        this.openTelemetry = openTelemetry;
        this.textMapGetter = textMapGetter;
        this.tracer = tracer;
        this.applicationName = applicationName;
        this.jsonPrinter = jsonPrinter;
    }

    /**
     * Init just to capture the reference to {@link ProcessorContext}.
     * <p>
     * <strong>Original documentation</strong>
     * </p>
     * {@inheritDoc}
     */
    @Override
    public void init(final ProcessorContext context) {
        getDelegate().init(context);
        this.context = context;
    }

    /**
     * Decorating process to create a span from the incoming metadata in headers, catch and log exceptions if any, update
     * and close the span at the end.
     * <p>
     * <strong>Original documentation</strong>
     * </p>
     * {@inheritDoc}
     */
    @Override
    public void process(Record record) {
        SpanBuilder spanBuilder = tracer.spanBuilder(applicationName);
        final TextMapPropagator propagator = openTelemetry.getPropagators().getTextMapPropagator();
        Scope parentScope = null;

        try {
            // going through all propagation field names defined in the OTel configuration
            // we look if any of them has been set with a non-null value in the headers of the incoming message
            if (propagator.fields()
                    .stream()
                    .map(record.headers()::lastHeader)
                    .anyMatch(Objects::nonNull)) {
                // if that is the case, let's extract a Context initialized with the parent trace id, span id
                // and baggage present as headers in the incoming message
                Context extractedContext = propagator.extract(Context.current(), record.headers(), textMapGetter);
                // use the context as parent span for the built span
                spanBuilder.setParent(extractedContext);
                // we clean the headers to avoid their propagation in any outgoing message (knowing that by
                // default Kafka Streams copies all headers of the incoming message into any outgoing message)
                propagator.fields().forEach(record.headers()::remove);
                // we make the parent context current to not loose the baggage
                parentScope = extractedContext.makeCurrent();
            }
            Span span = spanBuilder.startSpan();
            // baggage need to be explicitly set as current otherwise it is not propagated (baggage is independent of span
            // in opentelemetry) and actually lost as kafka headers are cleaned
            try (Scope ignored = span.makeCurrent()) {
                try {
                    getDelegate().process(record);
                    span.setStatus(StatusCode.OK);
                } catch (KafkaException e) {
                    // we got a Kafka exception, we record the exception in the span, log but rethrow the exception
                    // with the idea that it will be caught by one of the DLQ in error management
                    span.recordException(e);
                    span.setStatus(StatusCode.ERROR, e.getMessage());
                    logInputMessageMetadata(record);
                    throw e;
                } catch (RuntimeException e) { // NOSONAR
                    // very last resort, even the DLQs are not working, then we still record the exception and
                    // log the message but do not rethrow the exception otherwise we'd end up in an infinite loop
                    log.error("Runtime error caught while processing the message", e);
                    span.recordException(e);
                    span.setStatus(StatusCode.ERROR, e.getMessage());
                    logInputMessageMetadata(record);
                }
            } finally {
                span.end();
            }
        } finally {
            if (parentScope != null) {
                parentScope.close();
            }
        }
    }

    void logInputMessageMetadata(Record record) {
        if (log.isDebugEnabled()) {
            Map<String, String> headers = toMap(record.headers());
            LoggedRecord.LoggedRecordBuilder builder = LoggedRecord.builder()
                    .headers(headers)
                    .id(headers.get(KafkaStreamsProcessorHeaders.UUID))
                    .time(ZonedDateTime.ofInstant(Instant.ofEpochMilli(record.timestamp()), ZoneOffset.UTC))
                    .appId(context.applicationId());
            marshallValue(record, builder);
            extractMetadata(builder);
            log.debug("Input message is {}", builder.build());
        }
    }

    private static Map<String, String> toMap(org.apache.kafka.common.header.Headers headers) {
        return stream(headers.spliterator(), false)
                .collect(Collectors.toMap(Header::key, header -> new String(header.value(), Charset.defaultCharset())));
    }

    private void marshallValue(Record<?, ?> record, LoggedRecord.LoggedRecordBuilder builder) {
        if (record.value() instanceof MessageOrBuilder) {
            try {
                builder.value(jsonPrinter.print((MessageOrBuilder) record.value()));
            } catch (InvalidProtocolBufferException e) {
                log.error("Could not unmarshal to JSON", e);
            }
        } else {
            builder.value(record.value().toString());
        }
    }

    private void extractMetadata(LoggedRecord.LoggedRecordBuilder builder) {
        context.recordMetadata().ifPresent(metadata -> builder.topic(metadata.topic()).partition(metadata.partition()));
    }

    private interface Excludes {
        <KOut, VOut> void init(final ProcessorContext<KOut, VOut> context);

        <KIn, VIn> void process(Record<KIn, VIn> record);
    }
}
