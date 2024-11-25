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
package io.quarkiverse.kafkastreamsprocessor.impl.decorator.producer;

import static io.quarkiverse.kafkastreamsprocessor.impl.utils.KafkaHeaderUtils.getHeader;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.nullValue;

import java.nio.charset.StandardCharsets;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.opentelemetry.api.baggage.Baggage;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.sdk.trace.IdGenerator;
import io.quarkiverse.kafkastreamsprocessor.impl.utils.OpenTelemetryWithBaggageExtension;
import io.quarkiverse.kafkastreamsprocessor.propagation.KafkaTextMapSetter;

class TracingProducerInterceptorTest {
    @RegisterExtension
    static final OpenTelemetryWithBaggageExtension otel = OpenTelemetryWithBaggageExtension.create();

    KafkaTextMapSetter setter = new KafkaTextMapSetter();

    TracingProducerInterceptor interceptor = new TracingProducerInterceptor(otel.getOpenTelemetry(), setter);

    @Test
    void testTraceInjected() {
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>("topic", 0, "key".getBytes(),
                "value".getBytes(), new RecordHeaders().add("traceparent", "ll".getBytes(StandardCharsets.UTF_8))
                        .add("tracestate", "state".getBytes(StandardCharsets.UTF_8))
                        .add("baggage", "baggage".getBytes(StandardCharsets.UTF_8)));
        Span span = otel.getOpenTelemetry().getTracer("test").spanBuilder("test").startSpan();
        try (Scope ignored = span.makeCurrent()) {
            ProducerRecord<byte[], byte[]> output = interceptor.onSend(record);

            assertThat(getHeader(output, "traceparent"), containsString(span.getSpanContext().getSpanId()));
            assertThat(getHeader(output, "tracestate"), nullValue());
            assertThat(getHeader(output, "baggage"), nullValue());
        } finally {
            span.end();
        }

    }

    @Test
    void testStateAndBaggageAreInjected() {
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>("topic", 0, "key".getBytes(),
                "value".getBytes(), new RecordHeaders().add("traceparent", "ll".getBytes(StandardCharsets.UTF_8))
                        .add("tracestate", "state".getBytes(StandardCharsets.UTF_8))
                        .add("baggage", "baggage".getBytes(StandardCharsets.UTF_8)));
        Tracer tracer = otel.getOpenTelemetry().getTracer("test");
        TraceState state = TraceState.builder()
                .put("foo", "bar")
                .put("baz", "42")
                .build();
        SpanContext withTraceState = SpanContext.create(IdGenerator.random().generateTraceId(),
                IdGenerator.random().generateSpanId(),
                TraceFlags.getSampled(), state);
        Span span = tracer.spanBuilder("span")
                .setParent(Context.root().with(Span.wrap(withTraceState)))
                .startSpan();
        Baggage baggage = Baggage.builder()
                .put("picky", "frown")
                .put("abandoned", "ship")
                .build();
        try (Scope ignored = span.makeCurrent();
                Scope ignored2 = baggage.makeCurrent()) {
            ProducerRecord<byte[], byte[]> output = interceptor.onSend(record);

            assertThat(getHeader(output, "traceparent"), containsString(span.getSpanContext().getTraceId()));
            assertThat(getHeader(output, "tracestate"), both(containsString("foo=bar")).and(containsString("baz=42")));
            assertThat(getHeader(output, "baggage"),
                    both(containsString("picky=frown")).and(containsString("abandoned=ship")));
        } finally {
            span.end();
        }
    }

}
