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
package io.quarkiverse.kafkastreamsprocessor.sample.simple;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.github.daniel.shuy.kafka.protobuf.serde.KafkaProtobufDeserializer;
import com.github.daniel.shuy.kafka.protobuf.serde.KafkaProtobufSerializer;

import io.quarkiverse.kafkastreamsprocessor.sample.message.PingMessage.Ping;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.kafka.InjectKafkaCompanion;
import io.quarkus.test.kafka.KafkaCompanionResource;
import io.smallrye.reactive.messaging.kafka.companion.ConsumerBuilder;
import io.smallrye.reactive.messaging.kafka.companion.KafkaCompanion;
import io.smallrye.reactive.messaging.kafka.companion.ProducerBuilder;

/**
 * Blackbox test that can run both in JVM and native modes (@Inject and @ConfigProperty not allowed)
 */
@QuarkusTest
@TestProfile(ParallelPingProcessorQuarkusTest.ParallelPingProcessorQuarkusTestProfile.class)
@QuarkusTestResource(KafkaCompanionResource.class)
public class ParallelPingProcessorQuarkusTest {
    public static final String W3C_TRACE_ID = "traceparent";
    @InjectKafkaCompanion
    KafkaCompanion companion;

    String senderTopic = "ping-events";

    String consumerTopic = "pong-events";

    ProducerBuilder<String, Ping> producer;

    ConsumerBuilder<String, Ping> consumer;

    @BeforeEach
    public void setup() {
        consumer = companion.consumeWithDeserializers(new StringDeserializer(), new KafkaProtobufDeserializer<>(Ping.parser()))
                .withGroupId("test").withOffsetReset(OffsetResetStrategy.EARLIEST.toString()).withAutoCommit();
        producer = companion.produceWithSerializers(new StringSerializer(), new KafkaProtobufSerializer<>());
    }

    @AfterEach
    public void tearDown() {
        producer.close();
        consumer.close();
    }

    @Test
    public void multiPartitions() {
        // Send one message to each partition, they will be served each by one processor thread
        producer.fromRecords(new ProducerRecord<>(senderTopic, 0, null, Ping.newBuilder().setMessage("blabla").build()))
                .awaitCompletion(Duration.ofSeconds(1));
        producer.fromRecords(new ProducerRecord<>(senderTopic, 1, null, Ping.newBuilder().setMessage("blabla").build()))
                .awaitCompletion(Duration.ofSeconds(1));

        List<ConsumerRecord<String, Ping>> records = consumer.fromTopics(consumerTopic, 2)
                .awaitCompletion(Duration.ofSeconds(10)).getRecords();

        assertThat(records, hasSize(2));
        List<String> traceHeaderValues = StreamSupport.stream(records.spliterator(), false)
                .map(record -> new String(record.headers().lastHeader(W3C_TRACE_ID).value(), StandardCharsets.UTF_8))
                .collect(Collectors.toList());
        assertNotEquals(traceHeaderValues.get(0), traceHeaderValues.get(1));
    }

    public static class ParallelPingProcessorQuarkusTestProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of("quarkus.kafka-streams.num.stream.threads", "2");
        }
    }

}
