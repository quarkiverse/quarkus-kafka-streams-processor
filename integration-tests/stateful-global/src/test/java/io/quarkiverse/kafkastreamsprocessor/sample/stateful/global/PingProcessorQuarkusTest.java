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
package io.quarkiverse.kafkastreamsprocessor.sample.stateful.global;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
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

    String globalTopic = "global-topic";

    String globalTopicCapital = "global-topic-capital";

    KafkaProducer<String, Ping> producerPing;

    KafkaProducer<String, String> producerGlobalTopic;

    KafkaProducer<String, String> producerGlobalTopicCapital;

    KafkaConsumer<String, Ping> consumer;

    @KafkaBootstrapServers
    String kafkaBootstrapServers;

    @BeforeEach
    public void setup() {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(kafkaBootstrapServers, "test", "true");

        consumer = new KafkaConsumer(consumerProps, new StringDeserializer(),
                new KafkaProtobufDeserializer<>(Ping.parser()));
        consumer.subscribe(List.of(consumerTopic));

        Map<String, Object> producerProps = KafkaTestUtils.producerProps(kafkaBootstrapServers);
        producerPing = new KafkaProducer(producerProps, new StringSerializer(),
                new KafkaProtobufSerializer<>());

        producerGlobalTopic = new KafkaProducer<>(producerProps, new StringSerializer(),
                new StringSerializer());

        producerGlobalTopicCapital = new KafkaProducer<>(producerProps, new StringSerializer(),
                new StringSerializer());
    }

    @AfterEach
    public void tearDown() {
        producerPing.close();
        producerGlobalTopic.close();
        producerGlobalTopicCapital.close();
        consumer.close();
    }

    @Test
    void testGlobalStoreValueRetrieval() throws InterruptedException {
        producerPing.send(new ProducerRecord<>(senderTopic, "ID1", Ping.newBuilder().setMessage("whatever").build()));
        ConsumerRecord<String, Ping> receivedRecord = KafkaTestUtils.getSingleRecord(consumer, consumerTopic);
        assertThat(receivedRecord.value().getMessage(), containsString("Stored value for ID1 is null"));

        // Store two values using the two global topics
        producerGlobalTopic.send(new ProducerRecord<>(globalTopic, "ID1", "dont-capitalize-me"));
        producerGlobalTopicCapital.send(new ProducerRecord<>(globalTopicCapital, "ID1", "capitalize-me"));
        Thread.sleep(500L);
        producerPing.send(new ProducerRecord<>(senderTopic, "ID1", Ping.newBuilder().setMessage("whatever").build()));
        receivedRecord = KafkaTestUtils.getSingleRecord(consumer, consumerTopic);
        // Check that the value has been stored in the global store
        assertThat(receivedRecord.value().getMessage(),
                containsString("Stored value for ID1 is dont-capitalize-me and capitalized value is CAPITALIZE-ME"));

        // Check that the value stil exists in the global store
        producerPing.send(new ProducerRecord<>(senderTopic, "ID1", Ping.newBuilder().setMessage("whatever").build()));
        receivedRecord = KafkaTestUtils.getSingleRecord(consumer, consumerTopic);
        assertThat(receivedRecord.value().getMessage(),
                containsString("Stored value for ID1 is dont-capitalize-me and capitalized value is CAPITALIZE-ME"));

        // Store two new values using the two global topics
        producerGlobalTopic.send(new ProducerRecord<>(globalTopic, "ID1", "dont-capitalize-me-2"));
        producerGlobalTopicCapital.send(new ProducerRecord<>(globalTopicCapital, "ID1", "capitalize-me-2"));
        Thread.sleep(500L);
        producerPing.send(new ProducerRecord<>(senderTopic, "ID1", Ping.newBuilder().setMessage("whatever").build()));
        receivedRecord = KafkaTestUtils.getSingleRecord(consumer, consumerTopic);
        // Check that the value has been stored in the global store
        assertThat(receivedRecord.value().getMessage(),
                containsString("Stored value for ID1 is dont-capitalize-me-2 and capitalized value is CAPITALIZE-ME-2"));
    }
}
