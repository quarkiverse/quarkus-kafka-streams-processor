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
package io.quarkiverse.kafkastreamsprocessor.sample.customserde;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import jakarta.inject.Inject;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Durations;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import io.quarkus.test.junit.QuarkusTest;

@QuarkusTest
public class PingProcessorQuarkusTest {
    @ConfigProperty(name = "kafka.bootstrap.servers")
    String kafkaBootstrapServers;

    String senderTopic = "ping-events";

    String consumerTopic = "pong-events";

    KafkaProducer<String, CustomType> producer;

    KafkaConsumer<String, CustomType> consumer;

    @Inject
    CustomTypeSerde customTypeSerde;

    @BeforeEach
    public void setup() throws Exception {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(kafkaBootstrapServers, "test", "true");
        consumer = new KafkaConsumer<>(consumerProps, new StringDeserializer(), customTypeSerde.deserializer());
        consumer.subscribe(List.of(consumerTopic));

        Map<String, Object> producerProps = KafkaTestUtils.producerProps(kafkaBootstrapServers);
        producer = new KafkaProducer<>(producerProps, new StringSerializer(), customTypeSerde.serializer());
    }

    @AfterEach
    public void tearDown() {
        producer.close();
        consumer.close();
    }

    @Test
    public void testCount() {
        producer.send(new ProducerRecord<>(senderTopic, "1", new CustomType(1)));
        producer.flush();
        ConsumerRecord<String, CustomType> record = KafkaTestUtils.getSingleRecord(consumer, consumerTopic,
                Durations.FIVE_SECONDS);
        assertThat(((CustomType) record.value()).getValue(), equalTo(1));
    }

    @Test
    public void testHeaderError() {
        producer.send(new ProducerRecord<>(senderTopic, 0, "1", new CustomType(1),
                new RecordHeaders().add("custom-header", "error".getBytes(StandardCharsets.UTF_8))));
        producer.flush();
        assertThrows(IllegalStateException.class,
                () -> KafkaTestUtils.getSingleRecord(consumer, consumerTopic, Durations.FIVE_SECONDS));
    }
}
