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
package io.quarkiverse.kafkastreamsprocessor.sample.multioutput;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
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
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;

/**
 * Blackbox test that can run both in JVM and native modes (@Inject and @ConfigProperty not allowed)
 */
@QuarkusTest
@QuarkusTestResource(QuarkusIntegrationCompatibleKafkaDevServicesResource.class)
public class PingProcessorQuarkusTest {
    @KafkaBootstrapServers
    String kafkaBootstrapServers;

    String senderTopic = "ping-events";

    String consumerPongTopic = "pong-events";
    String consumerPangTopic = "pang-events";

    KafkaProducer<String, Ping> producer;

    KafkaConsumer<String, Ping> consumer;

    @BeforeEach
    public void setup() {
        producer = new KafkaProducer(KafkaTestUtils.producerProps(kafkaBootstrapServers), new StringSerializer(),
                new KafkaProtobufSerializer<>());
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(kafkaBootstrapServers, "test", "true");
        consumer = new KafkaConsumer(consumerProps, new StringDeserializer(),
                new KafkaProtobufDeserializer<>(Ping.parser()));
        consumer.subscribe(List.of(consumerPongTopic, consumerPangTopic));
    }

    @AfterEach
    public void tearDown() {
        producer.close(Durations.ONE_SECOND);
        consumer.close(Durations.ONE_SECOND);
    }

    @Test
    public void testNominalCase() {
        Ping ping = Ping.newBuilder().setMessage("world").build();

        producer.send(new ProducerRecord<>(senderTopic, ping));
        producer.flush();

        ConsumerRecord<String, Ping> record = KafkaTestUtils.getSingleRecord(consumer, consumerPongTopic,
                Durations.TEN_SECONDS);
        assertEquals("5", record.value().getMessage());
    }

    @Test
    public void testFunctionalError() {
        Ping ping = Ping.newBuilder().setMessage("error").build();

        producer.send(new ProducerRecord<>(senderTopic, ping));
        producer.flush();

        ConsumerRecord<String, Ping> record = KafkaTestUtils.getSingleRecord(consumer, consumerPangTopic,
                Durations.TEN_SECONDS);
        assertEquals("PANG!", record.value().getMessage());
    }
}
