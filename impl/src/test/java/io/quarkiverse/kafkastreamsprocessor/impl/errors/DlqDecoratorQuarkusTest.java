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
package io.quarkiverse.kafkastreamsprocessor.impl.errors;

import static io.quarkiverse.kafkastreamsprocessor.impl.protocol.KafkaStreamsProcessorHeaders.DLQ_CAUSE;
import static io.quarkiverse.kafkastreamsprocessor.impl.protocol.KafkaStreamsProcessorHeaders.DLQ_PARTITION;
import static io.quarkiverse.kafkastreamsprocessor.impl.protocol.KafkaStreamsProcessorHeaders.DLQ_REASON;
import static io.quarkiverse.kafkastreamsprocessor.impl.protocol.KafkaStreamsProcessorHeaders.DLQ_TOPIC;
import static io.quarkiverse.kafkastreamsprocessor.impl.protocol.KafkaStreamsProcessorHeaders.W3C_TRACE_ID;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.Alternative;
import jakarta.inject.Inject;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.github.daniel.shuy.kafka.protobuf.serde.KafkaProtobufDeserializer;
import com.github.daniel.shuy.kafka.protobuf.serde.KafkaProtobufSerializer;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.assertj.TracesAssert;
import io.quarkiverse.kafkastreamsprocessor.api.Processor;
import io.quarkiverse.kafkastreamsprocessor.api.decorator.outputrecord.OutputRecordInterceptor;
import io.quarkiverse.kafkastreamsprocessor.api.decorator.processor.AbstractProcessorDecorator;
import io.quarkiverse.kafkastreamsprocessor.api.decorator.processor.ProcessorDecoratorPriorities;
import io.quarkiverse.kafkastreamsprocessor.impl.utils.TestSpanExporter;
import io.quarkiverse.kafkastreamsprocessor.sample.message.PingMessage;
import io.quarkiverse.kafkastreamsprocessor.spi.properties.KStreamsProcessorConfig;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.kafka.InjectKafkaCompanion;
import io.quarkus.test.kafka.KafkaCompanionResource;
import io.smallrye.reactive.messaging.kafka.companion.ConsumerBuilder;
import io.smallrye.reactive.messaging.kafka.companion.KafkaCompanion;
import io.smallrye.reactive.messaging.kafka.companion.ProducerBuilder;

@QuarkusTest
@TestProfile(DlqDecoratorQuarkusTest.TestProfile.class)
@QuarkusTestResource(KafkaCompanionResource.class)
public class DlqDecoratorQuarkusTest {
    private static final String DLQ_TOPIC_NAME = "dlq-topic";
    private static final String INTERCEPT_AND_FAIL_MESSAGE = "Intercept&Fail";
    private static final String PROCESS_AND_FAIL_MESSAGE = "Process&Fail";
    private static final String PRODUCER_INTERCEPTOR_ADDED_HEADER_NAME = "producer-interceptor-added-header";
    private static final String PRODUCER_INTERCEPTOR_ADDED_HEADER_VALUE = "producer-interceptor-added-header-value";

    @Inject
    KStreamsProcessorConfig kStreamsProcessorConfig;

    @InjectKafkaCompanion
    KafkaCompanion companion;

    ProducerBuilder<String, PingMessage.Ping> producer;

    ConsumerBuilder<String, PingMessage.Ping> consumer;

    @Inject
    OpenTelemetry openTelemetry;

    @Inject
    TestSpanExporter testSpanExporter;

    @BeforeEach
    public void setup() {
        producer = companion.produceWithSerializers(new StringSerializer(), new KafkaProtobufSerializer<>());
        consumer = companion.consumeWithDeserializers(new StringDeserializer(),
                new KafkaProtobufDeserializer<>(PingMessage.Ping.parser()))
                .withGroupId("test").withOffsetReset(OffsetResetStrategy.EARLIEST.toString()).withAutoCommit();
    }

    @AfterEach
    public void tearDown() {
        producer.close();
        consumer.close();
        clearSpans();
    }

    private void clearSpans() {
        // force a flush to make sure there are no remaining spans still in the buffers
        ((OpenTelemetrySdk) openTelemetry).getSdkTracerProvider().forceFlush().join(10, TimeUnit.SECONDS);
        testSpanExporter.getSpans().clear();
    }

    @Test
    void successFlowShouldNotGoInDlqTopic() {
        PingMessage.Ping ping = PingMessage.Ping.newBuilder().setMessage("Hello World").build();
        producer.fromRecords(new ProducerRecord<>(kStreamsProcessorConfig.input().topic().get(), 0, "blabla", ping))
                .awaitCompletion(Duration.ofSeconds(1));

        ConsumerRecord<String, PingMessage.Ping> mirroredRecord = consumer
                .fromTopics(kStreamsProcessorConfig.output().topic().get(), 1)
                .awaitCompletion(Duration.ofSeconds(10)).getFirstRecord();

        assertThat(mirroredRecord.key(), equalTo("blabla"));
        assertThat(mirroredRecord.value().getMessage(), equalTo("Hello World"));
        assertThat(headerValue(mirroredRecord, PRODUCER_INTERCEPTOR_ADDED_HEADER_NAME),
                equalTo(PRODUCER_INTERCEPTOR_ADDED_HEADER_VALUE));

        ((OpenTelemetrySdk) openTelemetry).getSdkTracerProvider().forceFlush().join(10, TimeUnit.SECONDS);

        TracesAssert.assertThat(testSpanExporter.getSpans()).hasTracesSatisfyingExactly(
                trace -> trace.hasSpansSatisfyingExactly(
                        span -> span.hasEnded()));
    }

    @Test
    void processorErrorShouldGoInDlqTopic() {
        PingMessage.Ping ping = PingMessage.Ping.newBuilder().setMessage(PROCESS_AND_FAIL_MESSAGE).build();
        producer.fromRecords(new ProducerRecord<>(kStreamsProcessorConfig.input().topic().get(), 0, "blabla", ping,
                new RecordHeaders().add("header1", "value".getBytes(StandardCharsets.UTF_8))))
                .awaitCompletion(Duration.ofSeconds(1));

        ConsumerRecord<String, PingMessage.Ping> dlqRecord = consumer.fromTopics(DLQ_TOPIC_NAME, 1)
                .awaitCompletion(Duration.ofSeconds(10)).getFirstRecord();

        assertThat(dlqRecord.key(), equalTo("blabla"));
        assertThat(dlqRecord.value().getMessage(), equalTo(PROCESS_AND_FAIL_MESSAGE));
        assertThat(dlqRecord.headers().toArray().length, equalTo(6));
        assertThat(headerValue(dlqRecord, "header1"), equalTo("value"));
        assertThat(headerValue(dlqRecord, DLQ_REASON), equalTo("Processor code throwing exception"));
        assertThat(headerValue(dlqRecord, DLQ_CAUSE), equalTo("java.lang.Throwable"));
        assertThat(headerValue(dlqRecord, DLQ_PARTITION), equalTo("0"));
        assertThat(headerValue(dlqRecord, DLQ_TOPIC), equalTo(kStreamsProcessorConfig.input().topic().get()));
        assertThat(dlqRecord.headers().lastHeader(PRODUCER_INTERCEPTOR_ADDED_HEADER_NAME), nullValue());

        // ensure trace headers were injected into the DLQ record; we expect only the headers configured by
        // propagator to be actually propagated
        assertThat(dlqRecord.headers().lastHeader(W3C_TRACE_ID), notNullValue());

        ((OpenTelemetrySdk) openTelemetry).getSdkTracerProvider().forceFlush().join(10, TimeUnit.SECONDS);

        TracesAssert.assertThat(testSpanExporter.getSpans()).hasTracesSatisfyingExactly(
                trace -> trace.hasSpansSatisfyingExactly(
                        span -> span.hasException(new RuntimeException("Processor code throwing exception"))));
    }

    @Test
    void interceptorErrorShouldGoInDlqTopic() {
        PingMessage.Ping ping = PingMessage.Ping.newBuilder().setMessage(INTERCEPT_AND_FAIL_MESSAGE).build();
        producer.fromRecords(new ProducerRecord<>(kStreamsProcessorConfig.input().topic().get(), 0, "blabla", ping))
                .awaitCompletion(Duration.ofSeconds(1));

        ConsumerRecord<String, PingMessage.Ping> dlqRecord = consumer.fromTopics(DLQ_TOPIC_NAME, 1)
                .awaitCompletion(Duration.ofSeconds(10)).getFirstRecord();

        assertThat(dlqRecord.key(), equalTo("blabla"));
        assertThat(dlqRecord.value().getMessage(), equalTo(INTERCEPT_AND_FAIL_MESSAGE));
        assertThat(dlqRecord.headers().toArray().length, equalTo(5));
        assertThat(headerValue(dlqRecord, DLQ_REASON), equalTo("Interceptor code throwing exception"));
        assertThat(headerValue(dlqRecord, DLQ_CAUSE), equalTo("java.lang.Throwable"));
        assertThat(headerValue(dlqRecord, DLQ_PARTITION), equalTo("0"));
        assertThat(headerValue(dlqRecord, DLQ_TOPIC), equalTo(kStreamsProcessorConfig.input().topic().get()));
        assertThat(dlqRecord.headers().lastHeader(PRODUCER_INTERCEPTOR_ADDED_HEADER_NAME), nullValue());

        // ensure trace headers were injected into the DLQ record
        assertThat(dlqRecord.headers().lastHeader(W3C_TRACE_ID), notNullValue());

        ((OpenTelemetrySdk) openTelemetry).getSdkTracerProvider().forceFlush().join(10, TimeUnit.SECONDS);

        TracesAssert.assertThat(testSpanExporter.getSpans()).hasTracesSatisfyingExactly(
                trace -> trace.hasSpansSatisfyingExactly(
                        span -> span.hasException(new RuntimeException("Interceptor code throwing exception"))));
    }

    @Processor
    @Alternative
    public static class FailingProcessor extends ContextualProcessor<String, PingMessage.Ping, String, PingMessage.Ping> {

        @Override
        public void process(Record<String, PingMessage.Ping> record) {
            if (record.value().getMessage().equals(PROCESS_AND_FAIL_MESSAGE)) {
                throw new RuntimeException("Processor code throwing exception", new Throwable());
            } else {
                context().forward(record);
            }
        }
    }

    @Priority(ProcessorDecoratorPriorities.DLQ + 1)
    @Dependent
    public static class FailingProcessorDecorator extends AbstractProcessorDecorator {

        @Override
        public void process(Record record) {
            if (record.value() instanceof PingMessage.Ping message) {
                if (message.getMessage().equals(INTERCEPT_AND_FAIL_MESSAGE)) {
                    throw new RuntimeException("Interceptor code throwing exception", new Throwable());
                }
            }

            getDelegate().process(record);
        }
    }

    @Alternative
    @ApplicationScoped
    public static class PushHeaderProducerInterceptor implements OutputRecordInterceptor {

        @Override
        public Record interceptOutputRecord(Record record) {
            record.headers().add(PRODUCER_INTERCEPTOR_ADDED_HEADER_NAME,
                    PRODUCER_INTERCEPTOR_ADDED_HEADER_VALUE.getBytes(StandardCharsets.UTF_8));
            return record;
        }
    }

    public static class TestProfile implements QuarkusTestProfile {
        @Override
        public Set<Class<?>> getEnabledAlternatives() {
            return Set.of(FailingProcessor.class, PushHeaderProducerInterceptor.class);
        }

        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of("kafkastreamsprocessor.error-strategy", "dead-letter-queue",
                    "kafkastreamsprocessor.dlq.topic", DLQ_TOPIC_NAME);
        }
    }

    private String headerValue(ConsumerRecord<?, ?> record, String headerName) {
        return new String(record.headers().lastHeader(headerName).value(), StandardCharsets.UTF_8);
    }
}
