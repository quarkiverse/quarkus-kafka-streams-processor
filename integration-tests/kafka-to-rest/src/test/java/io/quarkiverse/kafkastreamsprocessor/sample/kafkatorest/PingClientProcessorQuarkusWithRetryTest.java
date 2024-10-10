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
package io.quarkiverse.kafkastreamsprocessor.sample.kafkatorest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

import jakarta.inject.Inject;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Durations;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import com.github.daniel.shuy.kafka.protobuf.serde.KafkaProtobufDeserializer;
import com.github.daniel.shuy.kafka.protobuf.serde.KafkaProtobufSerializer;

import io.quarkiverse.kafkastreamsprocessor.api.exception.RetryableException;
import io.quarkiverse.kafkastreamsprocessor.sample.message.PingMessage.Ping;
import io.quarkiverse.kafkastreamsprocessor.spi.properties.KStreamsProcessorConfig;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;

@QuarkusTest
class PingClientProcessorQuarkusWithRetryTest {

    @Inject
    KStreamsProcessorConfig kStreamsProcessorConfig;

    @ConfigProperty(name = "kafka.bootstrap.servers")
    String kafkaBootstrapServers;

    KafkaProducer<String, Ping> producer;

    KafkaConsumer<String, Ping> consumer;

    // Building on the features provided by QuarkusMock, Quarkus also allows users to effortlessly take advantage of
    // Mockito for mocking the beans supported by QuarkusMock. This functionality is available via the
    // @io.quarkus.test.junit.mockito.InjectMock annotation which is available in the quarkus-junit5-mockito dependency.
    // https://quarkus.io/guides/getting-started-testing#quarkus_mock
    @InjectMock
    @RestClient
    PingResource client;

    @BeforeEach
    public void setup() {
        producer = new KafkaProducer<>(KafkaTestUtils.producerProps(kafkaBootstrapServers), new StringSerializer(),
                new KafkaProtobufSerializer<>());
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(kafkaBootstrapServers, "test", "true");
        consumer = new KafkaConsumer<>(consumerProps, new StringDeserializer(),
                new KafkaProtobufDeserializer<>(Ping.parser()));
        consumer.subscribe(List.of(kStreamsProcessorConfig.output().topic().get()));
    }

    @AfterEach
    public void tearDown() {
        producer.close(Durations.ONE_SECOND);
        consumer.close(Durations.ONE_SECOND);
    }

    @Test
    void singleMessageWithRetry() {
        // response "PONG"
        when(client.ping())
                .thenThrow(mock(RetryableException.class))
                .thenThrow(mock(RetryableException.class))
                .thenThrow(mock(RetryableException.class))
                .thenReturn("PONG");

        producer.send(new ProducerRecord<>(kStreamsProcessorConfig.input().topic().get(),
                Ping.newBuilder().setMessage("hello").build()));
        producer.flush();

        ConsumerRecord<String, Ping> singleRecord = KafkaTestUtils.getSingleRecord(consumer,
                kStreamsProcessorConfig.output().topic().get(),
                Durations.TEN_SECONDS);
        consumer.commitSync();

        assertEquals("PONG of hello", singleRecord.value().getMessage());
        verify(client, times(4)).ping(); // 3 retry + 1
    }

}
