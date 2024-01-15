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
package io.quarkiverse.kafkastreamsprocessor.impl.decorator.processor;

import static java.util.stream.StreamSupport.stream;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import jakarta.inject.Inject;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import com.github.daniel.shuy.kafka.protobuf.serde.KafkaProtobufDeserializer;
import com.github.daniel.shuy.kafka.protobuf.serde.KafkaProtobufSerializer;

import io.quarkiverse.kafkastreamsprocessor.api.properties.KStreamsProcessorConfig;
import io.quarkiverse.kafkastreamsprocessor.impl.decorator.request.RequestScopeConsumerProcessor;
import io.quarkiverse.kafkastreamsprocessor.sample.message.PingMessage;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;

@QuarkusTest
@TestProfile(CdiRequestContextDecoratorQuarkusTest.CdiRequestContextTestProfile.class)
public class CdiRequestContextDecoratorQuarkusTest {
    @Inject
    KStreamsProcessorConfig kStreamsProcessorConfig;

    @ConfigProperty(name = "kafka.bootstrap.servers")
    String kafkaBootstrapServers;

    KafkaProducer<String, PingMessage.Ping> producer;

    KafkaConsumer<String, PingMessage.Ping> consumer;

    @BeforeEach
    public void setup() {
        producer = new KafkaProducer<>(KafkaTestUtils.producerProps(kafkaBootstrapServers),
                new StringSerializer(),
                new KafkaProtobufSerializer<>());
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(kafkaBootstrapServers,
                "test", "true");
        consumer = new KafkaConsumer<>(consumerProps, new StringDeserializer(),
                new KafkaProtobufDeserializer<>(PingMessage.Ping.parser()));
        consumer.subscribe(List.of(kStreamsProcessorConfig.output().topic().get()));
    }

    @AfterEach
    public void tearDown() {
        producer.close();
        consumer.close();
    }

    @Test
    void successiveMessagesShouldUseDifferentRequestScopedBean() {
        // The processor forwards a message containing "processorUuid:requestScopedBeanUuid"
        // Call it twice to verify that the requestScopedBeanUuid changes
        PingMessage.Ping ping = PingMessage.Ping.newBuilder().setMessage("Hello world").build();
        producer.send(new ProducerRecord<>(kStreamsProcessorConfig.input().topic().get(), ping));
        producer.send(new ProducerRecord<>(kStreamsProcessorConfig.input().topic().get(), ping));
        producer.flush();

        ConsumerRecords<String, PingMessage.Ping> consumerRecords = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(10),
                2);

        String[] records = stream(consumerRecords.spliterator(), false)
                .map(record -> record.value().getMessage())
                .toArray(String[]::new);

        String[] uuids1 = records[0].split(":");
        String[] uuids2 = records[1].split(":");

        // The processor instance is the same for both messages: IDs must match
        assertEquals(uuids1[0], uuids2[0]);
        // The RequestScopedBean instance must change
        assertNotEquals(uuids1[1], uuids2[1]);
    }

    public static class CdiRequestContextTestProfile implements QuarkusTestProfile {
        @Override
        public Set<Class<?>> getEnabledAlternatives() {
            return Collections.singleton(RequestScopeConsumerProcessor.class);
        }
    }
}
