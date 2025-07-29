package io.quarkiverse.kafkastreamsprocessor.sample.stateful.global;

import java.util.Map;

import jakarta.inject.Inject;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.assertj.TracesAssert;
import io.quarkiverse.kafkastreamsprocessor.propagation.KafkaTextMapSetter;
import io.quarkiverse.kafkastreamsprocessor.testframework.KafkaBootstrapServers;
import io.quarkiverse.kafkastreamsprocessor.testframework.QuarkusIntegrationCompatibleKafkaDevServicesResource;
import io.quarkiverse.kafkastreamsprocessor.testframework.StateDirCleaningResource;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;

@QuarkusTest
@QuarkusTestResource(value = StateDirCleaningResource.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(QuarkusIntegrationCompatibleKafkaDevServicesResource.class)
public class TracingGlobalProcessorQuarkusTest {

    String globalTopicCapital = "global-topic-capital";

    KafkaProducer<String, String> producerGlobalTopicCapital;

    @KafkaBootstrapServers
    String kafkaBootstrapServers;

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
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(kafkaBootstrapServers);

        producerGlobalTopicCapital = new KafkaProducer<>(producerProps, new StringSerializer(),
                new StringSerializer());

        clearSpans();
    }

    @AfterEach
    public void tearDown() {
        clearSpans();
    }

    @Test
    public void spanShouldBeCreatedW3C() throws InterruptedException {
        Span parentSpan = tracer.spanBuilder("parent").startSpan();
        try (Scope ignored = parentSpan.makeCurrent()) {
            RecordHeaders headers = new RecordHeaders();
            openTelemetry.getPropagators()
                    .getTextMapPropagator()
                    .inject(Context.current(), headers, kafkaTextMapSetter);

            Thread.sleep(1000L);

            producerGlobalTopicCapital.send(new ProducerRecord<>(globalTopicCapital, 0, "ID1", "capitalize-me", headers));

            Thread.sleep(1000L);
        } finally {
            parentSpan.end();
        }

        ((OpenTelemetrySdk) openTelemetry).getSdkTracerProvider().forceFlush();

        TracesAssert.assertThat(testSpanExporter.getSpans()).hasTracesSatisfyingExactly(
                trace -> trace.hasSpansSatisfyingExactly(
                        span -> span.hasSpanId(parentSpan.getSpanContext().getSpanId()).hasName("parent"),
                        span -> span.hasTraceId(parentSpan.getSpanContext().getTraceId())
                                .hasParentSpanId(parentSpan.getSpanContext().getSpanId())
                                .hasName("global-store-" + "store-data-capital")));
    }

    private void clearSpans() {
        // force a flush to make sure there are no remaining spans still in the buffers
        ((OpenTelemetrySdk) openTelemetry).getSdkTracerProvider().forceFlush();
        testSpanExporter.getSpans().clear();
    }
}
