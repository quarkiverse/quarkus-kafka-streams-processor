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
package io.quarkiverse.kafkastreamsprocessor.impl;

import static io.quarkiverse.kafkastreamsprocessor.impl.utils.KafkaHeaderUtils.getHeader;
import static io.quarkiverse.kafkastreamsprocessor.sample.message.PingMessage.Ping.newBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;

import jakarta.enterprise.inject.Alternative;
import jakarta.inject.Inject;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import com.github.daniel.shuy.kafka.protobuf.serde.KafkaProtobufDeserializer;
import com.github.daniel.shuy.kafka.protobuf.serde.KafkaProtobufSerializer;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.baggage.Baggage;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.assertj.TracesAssert;
import io.quarkiverse.kafkastreamsprocessor.api.Processor;
import io.quarkiverse.kafkastreamsprocessor.impl.utils.TestSpanExporter;
import io.quarkiverse.kafkastreamsprocessor.propagation.KafkaTextMapSetter;
import io.quarkiverse.kafkastreamsprocessor.sample.message.PingMessage;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import lombok.extern.slf4j.Slf4j;

@QuarkusTest
@TestProfile(TracingBaggageAndCustomSpanQuarkusTest.TestProfile.class)
public class TracingBaggageAndCustomSpanQuarkusTest {
    @ConfigProperty(name = "kafkastreamsprocessor.input.topic")
    String senderTopic;

    @ConfigProperty(name = "kafkastreamsprocessor.output.topic")
    String consumerTopic;

    @ConfigProperty(name = "kafka.bootstrap.servers")
    String bootstrapServers;

    KafkaProducer<String, PingMessage.Ping> producer;

    KafkaConsumer<String, PingMessage.Ping> consumer;

    @Inject
    OpenTelemetry openTelemetry;

    @Inject
    KafkaTextMapSetter kafkaTextMapSetter;

    @Inject
    Tracer tracer;

    @Inject
    TestSpanExporter testSpanExporter;

    @BeforeEach
    public void setup() {
        producer = new KafkaProducer<>(KafkaTestUtils.producerProps(bootstrapServers), new StringSerializer(),
                new KafkaProtobufSerializer<>());
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(bootstrapServers, "test", "true");
        consumer = new KafkaConsumer<>(consumerProps, new StringDeserializer(),
                new KafkaProtobufDeserializer<>(PingMessage.Ping.parser()));
        consumer.subscribe(List.of(consumerTopic));

        clearSpans();
    }

    private void clearSpans() {
        // force a flush to make sure there are no remaining spans still in the buffers
        ((OpenTelemetrySdk) openTelemetry).getSdkTracerProvider().forceFlush();
        testSpanExporter.getSpans().clear();
    }

    @AfterEach
    public void tearDown() throws Exception {
        clearSpans();
        producer.close();
        consumer.close();
    }

    @Test
    public void baggageModifiedPropagatedAndExtraSpanCreated() {
        ConsumerRecord<String, PingMessage.Ping> receivedRecord;
        Span span = tracer.spanBuilder("god").startSpan();
        try (Scope ignored = span.makeCurrent();
                Scope ignored2 = Baggage.current().toBuilder().put("key1", "value1").build().makeCurrent()) {
            PingMessage.Ping input = PingMessage.Ping.newBuilder().setMessage("world").build();
            RecordHeaders recordHeaders = new RecordHeaders();
            openTelemetry.getPropagators()
                    .getTextMapPropagator()
                    .inject(Context.current(), recordHeaders, kafkaTextMapSetter);
            ProducerRecord<String, PingMessage.Ping> sentRecord = new ProducerRecord<>(senderTopic, 0, "key", input,
                    recordHeaders);

            producer.send(sentRecord);
            producer.flush();

            receivedRecord = KafkaTestUtils.getSingleRecord(consumer, consumerTopic);

            assertThat(getHeader(receivedRecord, "baggage"), containsString("key1=value1"));
            assertThat(getHeader(receivedRecord, "baggage"), containsString("key2=value2"));
        } finally {
            span.end();
        }

        ((OpenTelemetrySdk) openTelemetry).getSdkTracerProvider().forceFlush();

        TracesAssert.assertThat(testSpanExporter.getSpans())
                .hasTracesSatisfyingExactly(
                        trace -> trace.hasSpansSatisfyingExactly(
                                s -> s.hasSpanId(span.getSpanContext().getSpanId()),
                                s -> s.hasTraceId(span.getSpanContext().getTraceId())
                                        .hasParentSpanId(span.getSpanContext().getSpanId()),
                                s -> s.hasTraceId(span.getSpanContext().getTraceId())
                                        .hasAttribute(AttributeKey.stringKey("an-attribute"), "a-value")
                                        .hasSpanId(
                                                new String(receivedRecord.headers().lastHeader("traceparent").value(),
                                                        StandardCharsets.UTF_8)
                                                        .split("-")[2])));
    }

    @Processor
    @Alternative
    @Slf4j
    public static class TestProcessor extends ContextualProcessor<String, PingMessage.Ping, String, PingMessage.Ping> {
        @Inject
        Tracer tracer;

        @Override
        public void process(Record<String, PingMessage.Ping> record) {
            Span span = tracer.spanBuilder("custom span")
                    .setAttribute("an-attribute", "a-value")
                    .startSpan();
            try (Scope ignored2 = span.makeCurrent();
                    Scope ignored = Baggage.current().toBuilder().put("key2", "value2").build().makeCurrent()) {
                context().forward(record);
            } finally {
                span.end();
            }
        }
    }

    public static class TestProfile implements QuarkusTestProfile {
        @Override
        public Set<Class<?>> getEnabledAlternatives() {
            return Set.of(TestProcessor.class);
        }
    }
}
