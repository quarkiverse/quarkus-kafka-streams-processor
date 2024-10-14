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

import static io.quarkiverse.kafkastreamsprocessor.sample.message.PingMessage.Ping.newBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import jakarta.enterprise.inject.Alternative;
import jakarta.inject.Inject;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import com.github.daniel.shuy.kafka.protobuf.serde.KafkaProtobufDeserializer;
import com.github.daniel.shuy.kafka.protobuf.serde.KafkaProtobufSerializer;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.assertj.TracesAssert;
import io.quarkiverse.kafkastreamsprocessor.api.Processor;
import io.quarkiverse.kafkastreamsprocessor.impl.protocol.KafkaStreamsProcessorHeaders;
import io.quarkiverse.kafkastreamsprocessor.impl.utils.TestSpanExporter;
import io.quarkiverse.kafkastreamsprocessor.propagation.KafkaTextMapSetter;
import io.quarkiverse.kafkastreamsprocessor.sample.message.PingMessage;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import lombok.extern.slf4j.Slf4j;

@QuarkusTest
@TestProfile(TracingQuarkusTest.TestProfile.class)
public class TracingQuarkusTest {
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
    public void tracingShouldBePropagatedW3C() {
        Span parentSpan = tracer.spanBuilder("parent").startSpan();
        try (Scope parentScope = parentSpan.makeCurrent()) {
            PingMessage.Ping input = newBuilder().setMessage("world").build();
            RecordHeaders headers = new RecordHeaders();
            openTelemetry.getPropagators()
                    .getTextMapPropagator()
                    .inject(Context.current(), headers, kafkaTextMapSetter);
            ProducerRecord<String, PingMessage.Ping> record = new ProducerRecord<>(senderTopic, 0, "key", input, headers);
            producer.send(record);

            ConsumerRecord<String, PingMessage.Ping> singleRecord = KafkaTestUtils.getSingleRecord(consumer, consumerTopic,
                    Duration.ofSeconds(5));
            MatcherAssert.assertThat(toMap(singleRecord.headers()),
                    hasEntry(equalTo(KafkaStreamsProcessorHeaders.W3C_TRACE_ID),
                            containsString(parentSpan.getSpanContext().getTraceId())));
        } finally {
            parentSpan.end();
        }
    }

    private static Map<String, String> toMap(Iterable<Header> headers) {
        Map<String, String> result = new HashMap<>();
        for (Header h : headers) {
            result.put(h.key(), new String(h.value(), StandardCharsets.UTF_8));
        }
        return result;
    }

    @Test
    public void spanShouldBeCreatedW3C() {

        Span parentSpan = tracer.spanBuilder("parent").startSpan();
        try (Scope ignored = parentSpan.makeCurrent()) {
            PingMessage.Ping input = newBuilder().setMessage("world").build();
            RecordHeaders headers = new RecordHeaders();
            openTelemetry.getPropagators()
                    .getTextMapPropagator()
                    .inject(Context.current(), headers, kafkaTextMapSetter);
            ProducerRecord<String, PingMessage.Ping> record = new ProducerRecord<>(senderTopic, 0, "key", input, headers);
            producer.send(record);

            KafkaTestUtils.getSingleRecord(consumer, consumerTopic, Duration.ofSeconds(5000));
        } finally {
            parentSpan.end();
        }

        ((OpenTelemetrySdk) openTelemetry).getSdkTracerProvider().forceFlush();

        TracesAssert.assertThat(testSpanExporter.getSpans()).hasTracesSatisfyingExactly(
                trace -> trace.hasSpansSatisfyingExactly(
                        span -> span.hasSpanId(parentSpan.getSpanContext().getSpanId()),
                        span -> span.hasTraceId(parentSpan.getSpanContext().getTraceId())
                                .hasParentSpanId(parentSpan.getSpanContext().getSpanId())));
    }

    @Processor
    @Alternative
    @Slf4j
    public static class TestProcessor extends ContextualProcessor<String, PingMessage.Ping, String, PingMessage.Ping> {
        @Inject
        Tracer tracer;

        @Override
        public void process(Record<String, PingMessage.Ping> record) {
            context().forward(record);
        }
    }

    public static class TestProfile implements QuarkusTestProfile {
        @Override
        public Set<Class<?>> getEnabledAlternatives() {
            return Set.of(TestProcessor.class);
        }
    }
}
