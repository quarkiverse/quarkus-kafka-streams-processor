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

import static io.restassured.RestAssured.given;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import jakarta.enterprise.inject.Alternative;
import jakarta.inject.Inject;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
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

import io.micrometer.core.instrument.MeterRegistry;
import io.quarkiverse.kafkastreamsprocessor.api.Processor;
import io.quarkiverse.kafkastreamsprocessor.sample.message.PingMessage;
import io.quarkiverse.kafkastreamsprocessor.spi.properties.KStreamsProcessorConfig;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.restassured.http.ContentType;

@QuarkusTest
@TestProfile(LogAndSendToDlqExceptionHandlerDelegateQuarkusTest.TestProfile.class)
class LogAndSendToDlqExceptionHandlerDelegateQuarkusTest {
    private static final String DLQ_TOPIC = "dlq-topic";

    @Inject
    KStreamsProcessorConfig kStreamsProcessorConfig;

    @Inject
    MeterRegistry registry;

    @ConfigProperty(name = "kafka.bootstrap.servers")
    String kafkaBootstrapServers;

    KafkaProducer<String, PingMessage.Ping> producer;

    KafkaConsumer<String, PingMessage.Ping> consumer;

    KafkaConsumer<byte[], byte[]> dlqConsumer;

    @BeforeEach
    public void setup() {
        registry.clear();

        producer = new KafkaProducer<>(KafkaTestUtils.producerProps(kafkaBootstrapServers), new StringSerializer(),
                new KafkaProtobufSerializer<>() {
                    // Generate invalid protobuf messages to trigger a deserialization error
                    @Override
                    public byte[] serialize(String topic, PingMessage.Ping data) {
                        return "InvalidProtobufPayload".getBytes(StandardCharsets.UTF_8);
                    }
                });

        Map<String, Object> dlqConsumerProps = KafkaTestUtils.consumerProps(kafkaBootstrapServers, "dlq", "true");
        dlqConsumer = new KafkaConsumer<>(dlqConsumerProps, new ByteArrayDeserializer(),
                new ByteArrayDeserializer());

        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(kafkaBootstrapServers, "test", "true");
        consumer = new KafkaConsumer<>(consumerProps, new StringDeserializer(),
                new KafkaProtobufDeserializer<>(PingMessage.Ping.parser()));
    }

    @AfterEach
    public void tearDown() {
        producer.close();
        consumer.close();
        dlqConsumer.close();
    }

    @Test
    void deserializationErrorShouldGoInDlqTopic() throws Exception {
        consumer.subscribe(List.of(kStreamsProcessorConfig.output().topic().get()));
        dlqConsumer.subscribe(List.of(DLQ_TOPIC));

        PingMessage.Ping ping = PingMessage.Ping.newBuilder().setMessage("WillBeCorruptedBySerializer").build();
        producer.send(
                new ProducerRecord<String, PingMessage.Ping>(kStreamsProcessorConfig.input().topic().get(), 0,
                        null,
                        ping));
        producer.flush();

        ConsumerRecords<byte[], byte[]> dlqRecords = KafkaTestUtils.getRecords(dlqConsumer, Duration.ofSeconds(10), 1);

        assertEquals(1, dlqRecords.count(), "We do not have 1 corrupt protobuf message in the DLQ");

        double globalDlqMessagesSent = getMetricAsFloat("\"kafkastreamsprocessor.global.dlq.sent\"");
        assertThat(globalDlqMessagesSent, closeTo(0.0d, 0.1d));

        double dlqMessagesSent = getMetricAsFloat("\"kafkastreamsprocessor.dlq.sent\"");
        assertThat(dlqMessagesSent, closeTo(1.0d, 0.1d));
    }

    public static class TestProfile implements QuarkusTestProfile {
        @Override
        public Set<Class<?>> getEnabledAlternatives() {
            return Collections.singleton(TestProcessor.class);
        }

        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of("kafkastreamsprocessor.error-strategy", "dead-letter-queue",
                    "kafkastreamsprocessor.dlq.topic", DLQ_TOPIC);
        }
    }

    @Processor
    @Alternative
    public static class TestProcessor extends ContextualProcessor<String, PingMessage.Ping, String, PingMessage.Ping> {

        @Override
        public void process(Record<String, PingMessage.Ping> record) {
            context().forward(record);
        }
    }

    private float getMetricAsFloat(String jsonPath) {
        return given().accept(ContentType.JSON)
                .when()
                .get("/metrics")
                .body()
                .jsonPath()
                .getFloat(jsonPath);
    }
}
