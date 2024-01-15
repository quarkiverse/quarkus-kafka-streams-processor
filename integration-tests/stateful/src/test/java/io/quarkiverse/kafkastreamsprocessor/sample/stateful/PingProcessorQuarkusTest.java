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
package io.quarkiverse.kafkastreamsprocessor.sample.stateful;

import static io.restassured.RestAssured.given;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import jakarta.inject.Inject;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Durations;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import com.github.daniel.shuy.kafka.protobuf.serde.KafkaProtobufDeserializer;
import com.github.daniel.shuy.kafka.protobuf.serde.KafkaProtobufSerializer;

import io.quarkiverse.kafkastreamsprocessor.sample.message.PingMessage.Ping;
import io.quarkiverse.kafkastreamsprocessor.testframework.KafkaBootstrapServers;
import io.quarkiverse.kafkastreamsprocessor.testframework.QuarkusIntegrationCompatibleKafkaDevServicesResource;
import io.quarkiverse.kafkastreamsprocessor.testframework.StateDirCleaningResource;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;

/**
 * Blackbox test that can run both in JVM and native modes (@Inject and @ConfigProperty not allowed)
 */
@QuarkusTest
@QuarkusTestResource(StateDirCleaningResource.class)
@QuarkusTestResource(QuarkusIntegrationCompatibleKafkaDevServicesResource.class)
class PingProcessorQuarkusTest {
    String senderTopic = "ping-events";

    String consumerTopic = "pong-events";

    String storeTopic = "pingapp-ping-data-changelog";

    KafkaProducer<String, Ping> producer;

    KafkaConsumer<String, Ping> consumer;

    @KafkaBootstrapServers
    String kafkaBootstrapServers;

    @BeforeEach
    public void setup() throws IOException {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(kafkaBootstrapServers, "test", "true");

        consumer = new KafkaConsumer(consumerProps, new StringDeserializer(),
                new KafkaProtobufDeserializer<>(Ping.parser()));
        consumer.subscribe(List.of(consumerTopic));

        Map<String, Object> producerProps = KafkaTestUtils.producerProps(kafkaBootstrapServers);
        producer = new KafkaProducer(producerProps, new StringSerializer(),
                new KafkaProtobufSerializer<>());
    }

    @AfterEach
    public void tearDown() {
        producer.close();
        consumer.close();
    }

    @Test
    void testHistory() throws Exception {
        producer.send(new ProducerRecord<>(senderTopic, "ID1", Ping.newBuilder().setMessage("value1").build()));
        // forcing 1 millisecond between each message so the DuplicateValuesPunctuator is executed at every message
        // reception
        Thread.sleep(1L);
        producer.send(new ProducerRecord<>(senderTopic, "ID2", Ping.newBuilder().setMessage("value1").build()));
        Thread.sleep(1L);
        producer.send(new ProducerRecord<>(senderTopic, "ID1", Ping.newBuilder().setMessage("value2").build()));
        Thread.sleep(1L);
        producer.send(new ProducerRecord<>(senderTopic, "ID1", Ping.newBuilder().setMessage("value3").build()));
        Thread.sleep(1L);
        producer.send(new ProducerRecord<>(senderTopic, "ID2", Ping.newBuilder().setMessage("value2").build()));
        Thread.sleep(1L);
        producer.flush();

        ConsumerRecords<String, Ping> records = KafkaTestUtils.getRecords(consumer, Durations.TEN_SECONDS, 5);
        assertThat(
                StreamSupport.stream(records.spliterator(), false)
                        .map(ConsumerRecord::value)
                        .map(Ping::getMessage)
                        .collect(Collectors.toList()),
                contains("Store initialization OK for ID1", // value1 inserted with key ID1
                        "Store initialization OK for ID2", // value1 inserted with key ID2
                        // punctuator runs and finds value1 stored with ID1 and ID2, deletes ID2 entry
                        "Previous value for ID1 is value1", // changing value for ID1 to value2
                        "Previous value for ID1 is value2", // changing again key ID2 with value value3
                        "Store initialization OK for ID2")); // reinserting key ID2 with value value2

        given().when()
                .get("/metrics")
                .then()
                .log()
                .ifValidationFails()
                .statusCode(200)
                .body(containsString("kafkastreamsprocessor_punctuation_errors_total 1.0"));
    }
}
