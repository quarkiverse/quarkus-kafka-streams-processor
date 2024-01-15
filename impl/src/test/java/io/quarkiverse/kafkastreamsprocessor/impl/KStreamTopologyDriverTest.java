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

import static io.opentelemetry.sdk.testing.assertj.TracesAssert.assertThat;
import static io.quarkiverse.kafkastreamsprocessor.sample.message.PingMessage.Ping.newBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import jakarta.enterprise.inject.Alternative;
import jakarta.inject.Inject;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.test.TestRecord;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.github.daniel.shuy.kafka.protobuf.serde.KafkaProtobufDeserializer;
import com.github.daniel.shuy.kafka.protobuf.serde.KafkaProtobufSerializer;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.quarkiverse.kafkastreamsprocessor.api.Processor;
import io.quarkiverse.kafkastreamsprocessor.api.properties.KStreamsProcessorConfig;
import io.quarkiverse.kafkastreamsprocessor.impl.protocol.KafkaStreamsProcessorHeaders;
import io.quarkiverse.kafkastreamsprocessor.impl.utils.TestSpanExporter;
import io.quarkiverse.kafkastreamsprocessor.propagation.KafkaTextMapSetter;
import io.quarkiverse.kafkastreamsprocessor.sample.message.PingMessage.Ping;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import lombok.extern.slf4j.Slf4j;

@QuarkusTest
@TestProfile(KStreamTopologyDriverTest.SimpleProtobufProcessorProfile.class)
public class KStreamTopologyDriverTest {
    @Inject
    KafkaTextMapSetter kafkaTextMapSetter;

    @Inject
    Topology topology;

    TopologyTestDriver testDriver;

    @Inject
    KStreamsProcessorConfig kStreamsProcessorConfig;

    TestInputTopic<String, Ping> testInputTopic;

    TestOutputTopic<String, Ping> testOutputTopic;

    @Inject
    Tracer tracer;

    @Inject
    OpenTelemetry openTelemetry;

    @Inject
    TestSpanExporter testSpanExporter;

    @BeforeEach
    public void setup() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        testDriver = new TopologyTestDriver(topology, config);
        testInputTopic = testDriver.createInputTopic(kStreamsProcessorConfig.input().topic().get(), new StringSerializer(),
                new KafkaProtobufSerializer<Ping>());
        testOutputTopic = testDriver.createOutputTopic(kStreamsProcessorConfig.output().topic().get(),
                new StringDeserializer(),
                new KafkaProtobufDeserializer<>(Ping.parser()));

        clearSpans();
    }

    @AfterEach
    public void clearOtel() {
        clearSpans();
    }

    private void clearSpans() {
        // force a flush to make sure there are no remaining spans still in the buffers
        ((OpenTelemetrySdk) openTelemetry).getSdkTracerProvider().forceFlush();
        testSpanExporter.getSpans().clear();
    }

    @Test
    public void topologyShouldOutputMessage() {
        Ping input = newBuilder().setMessage("world").build();
        testInputTopic.pipeInput(input);
        assertEquals("world", testOutputTopic.readRecord().value().getMessage());
    }

    @Test
    public void shouldManageRuntimeException() {
        Ping nullInput = newBuilder().setMessage("null").build();
        testInputTopic.pipeInput(nullInput);
        assertTrue(testOutputTopic.isEmpty());
    }

    @Test
    public void tracingShouldBePropagatedW3C() {
        Span parentSpan = tracer.spanBuilder("parent").startSpan();
        try (Scope parentScope = parentSpan.makeCurrent()) {

            Ping input = newBuilder().setMessage("world").build();
            RecordHeaders headers = new RecordHeaders();
            openTelemetry.getPropagators()
                    .getTextMapPropagator()
                    .inject(Context.current(), headers, kafkaTextMapSetter);
            TestRecord record = new TestRecord("key", input, headers);
            testInputTopic.pipeInput(record);
            MatcherAssert.assertThat(toMap(testOutputTopic.readRecord().getHeaders()),
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
            Ping input = newBuilder().setMessage("world").build();
            RecordHeaders headers = new RecordHeaders();
            openTelemetry.getPropagators()
                    .getTextMapPropagator()
                    .inject(Context.current(), headers, kafkaTextMapSetter);
            TestRecord<String, Ping> record = new TestRecord<>("key", input, headers);

            testInputTopic.pipeInput(record);
            testOutputTopic.readRecord();
        } finally {
            parentSpan.end();
        }

        ((OpenTelemetrySdk) openTelemetry).getSdkTracerProvider().forceFlush();

        assertThat(testSpanExporter.getSpans()).hasTracesSatisfyingExactly(
                trace -> trace.hasSpansSatisfyingExactly(
                        span -> span.hasSpanId(parentSpan.getSpanContext().getSpanId()),
                        span -> span.hasTraceId(parentSpan.getSpanContext().getTraceId())
                                .hasParentSpanId(parentSpan.getSpanContext().getSpanId())));
    }

    @Processor
    @Alternative
    @Slf4j
    public static class TestProcessor extends ContextualProcessor<String, Ping, String, Ping> {

        @Override
        public void process(Record<String, Ping> record) {
            log.info("Processing...");
            if ("null".equals(record.value().getMessage())) {
                throw new TestException();
            }
            context().forward(record);
        }
    }

    public static class SimpleProtobufProcessorProfile implements QuarkusTestProfile {
        @Override
        public Set<Class<?>> getEnabledAlternatives() {
            return Set.of(TestProcessor.class);
        }
    }
}
