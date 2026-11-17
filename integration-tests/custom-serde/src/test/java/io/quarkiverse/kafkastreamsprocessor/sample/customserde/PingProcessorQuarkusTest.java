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
import java.time.Duration;

import jakarta.inject.Inject;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Durations;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.kafka.InjectKafkaCompanion;
import io.quarkus.test.kafka.KafkaCompanionResource;
import io.smallrye.reactive.messaging.kafka.companion.ConsumerBuilder;
import io.smallrye.reactive.messaging.kafka.companion.KafkaCompanion;
import io.smallrye.reactive.messaging.kafka.companion.ProducerBuilder;

@QuarkusTest
@QuarkusTestResource(KafkaCompanionResource.class)
public class PingProcessorQuarkusTest {
    @InjectKafkaCompanion
    KafkaCompanion companion;

    String senderTopic = "ping-events";

    String consumerTopic = "pong-events";

    ProducerBuilder<String, CustomType> producer;

    ConsumerBuilder<String, CustomType> consumer;

    @Inject
    CustomTypeSerde customTypeSerde;

    @BeforeEach
    public void setup() throws Exception {
        consumer = companion.consumeWithDeserializers(new StringDeserializer(), customTypeSerde.deserializer())
                .withGroupId("test").withOffsetReset(OffsetResetStrategy.EARLIEST.toString()).withAutoCommit();
        producer = companion.produceWithSerializers(new StringSerializer(), customTypeSerde.serializer());
    }

    @AfterEach
    public void tearDown() {
        producer.close();
        consumer.close();
    }

    @Test
    public void testCount() {
        producer.fromRecords(new ProducerRecord<>(senderTopic, "1", new CustomType(1))).awaitCompletion(Duration.ofSeconds(1));
        ConsumerRecord<String, CustomType> record = consumer.fromTopics(consumerTopic, 1)
                .awaitCompletion(Durations.FIVE_SECONDS).getFirstRecord();
        assertThat(((CustomType) record.value()).getValue(), equalTo(1));
    }

    @Test
    public void testHeaderError() {
        producer.fromRecords(new ProducerRecord<>(senderTopic, 0, "1", new CustomType(1),
                new RecordHeaders().add("custom-header", "error".getBytes(StandardCharsets.UTF_8))))
                .awaitCompletion(Duration.ofSeconds(1));
        assertThrows(AssertionError.class,
                () -> consumer.fromTopics(consumerTopic, 1).awaitCompletion(Durations.FIVE_SECONDS).getFirstRecord());
    }
}
