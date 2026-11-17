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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Durations;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.github.daniel.shuy.kafka.protobuf.serde.KafkaProtobufDeserializer;
import com.github.daniel.shuy.kafka.protobuf.serde.KafkaProtobufSerializer;

import io.quarkiverse.kafkastreamsprocessor.sample.message.PingMessage.Ping;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.kafka.InjectKafkaCompanion;
import io.quarkus.test.kafka.KafkaCompanionResource;
import io.smallrye.reactive.messaging.kafka.companion.ConsumerBuilder;
import io.smallrye.reactive.messaging.kafka.companion.KafkaCompanion;
import io.smallrye.reactive.messaging.kafka.companion.ProducerBuilder;

/**
 * Blackbox test that can run both in JVM and native modes (@Inject and @ConfigProperty not allowed)
 */
@QuarkusTest
@QuarkusTestResource(KafkaCompanionResource.class)
public class PingProcessorQuarkusTest {
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
    public void testCount() {
        producer.fromRecords(new ProducerRecord<>(senderTopic, Ping.newBuilder().setMessage("world").build()))
                .awaitCompletion(Duration.ofSeconds(1));
        ConsumerRecord<String, Ping> record = consumer.fromTopics(consumerTopic, 1).awaitCompletion(Durations.FIVE_SECONDS)
                .getFirstRecord();
        assertEquals("5", record.value().getMessage());
    }

}
