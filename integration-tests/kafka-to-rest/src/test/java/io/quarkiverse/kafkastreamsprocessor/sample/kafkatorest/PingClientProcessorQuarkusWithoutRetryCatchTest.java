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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import java.time.Duration;

import jakarta.inject.Inject;

import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Durations;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.github.daniel.shuy.kafka.protobuf.serde.KafkaProtobufDeserializer;
import com.github.daniel.shuy.kafka.protobuf.serde.KafkaProtobufSerializer;

import io.quarkiverse.kafkastreamsprocessor.sample.message.PingMessage.Ping;
import io.quarkiverse.kafkastreamsprocessor.spi.properties.KStreamsProcessorConfig;
import io.quarkus.test.InjectMock;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.kafka.InjectKafkaCompanion;
import io.quarkus.test.kafka.KafkaCompanionResource;
import io.smallrye.reactive.messaging.kafka.companion.ConsumerBuilder;
import io.smallrye.reactive.messaging.kafka.companion.KafkaCompanion;
import io.smallrye.reactive.messaging.kafka.companion.ProducerBuilder;

@QuarkusTest
@QuarkusTestResource(KafkaCompanionResource.class)
class PingClientProcessorQuarkusWithoutRetryCatchTest {

    @Inject
    KStreamsProcessorConfig kStreamsProcessorConfig;

    @InjectKafkaCompanion
    KafkaCompanion companion;

    ProducerBuilder<String, Ping> producer;

    ConsumerBuilder<String, Ping> consumer;

    // Building on the features provided by QuarkusMock, Quarkus also allows users to effortlessly take advantage of
    // Mockito for mocking the beans supported by QuarkusMock. This functionality is available via the
    // @io.quarkus.test.junit.mockito.InjectMock annotation which is available in the quarkus-junit5-mockito dependency.
    // https://quarkus.io/guides/getting-started-testing#quarkus_mock
    @InjectMock
    @RestClient
    PingResource client;

    @BeforeEach
    public void setup() {
        producer = companion.produceWithSerializers(new StringSerializer(), new KafkaProtobufSerializer<>());
        consumer = companion.consumeWithDeserializers(new StringDeserializer(), new KafkaProtobufDeserializer<>(Ping.parser()))
                .withGroupId("test").withOffsetReset(OffsetResetStrategy.EARLIEST.toString()).withAutoCommit();
    }

    @AfterEach
    public void tearDown() {
        producer.close();
        consumer.close();
    }

    @Test
    void singleMessageWithoutRetry() {
        // throw Exception
        when(client.ping())
                .thenThrow(new RuntimeException());

        producer.fromRecords(new ProducerRecord<>(kStreamsProcessorConfig.input().topic().get(),
                Ping.newBuilder().setMessage("hello").build())).awaitCompletion(Duration.ofSeconds(1));

        assertThrows(AssertionError.class, () -> {
            consumer.fromTopics(kStreamsProcessorConfig.output().topic().get(), 1)
                    .awaitCompletion(Durations.TEN_SECONDS).getFirstRecord();
        });
    }
}
