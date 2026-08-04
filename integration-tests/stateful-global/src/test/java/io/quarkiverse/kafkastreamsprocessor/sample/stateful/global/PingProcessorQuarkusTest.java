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

import java.time.Duration;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.github.daniel.shuy.kafka.protobuf.serde.KafkaProtobufDeserializer;
import com.github.daniel.shuy.kafka.protobuf.serde.KafkaProtobufSerializer;

import io.quarkiverse.kafkastreamsprocessor.sample.message.PingMessage.Ping;
import io.quarkiverse.kafkastreamsprocessor.testframework.StateDirCleaningResource;
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
@QuarkusTestResource(value = StateDirCleaningResource.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(KafkaCompanionResource.class)
class PingProcessorQuarkusTest {
    String senderTopic = "ping-events";

    String consumerTopic = "pong-events";

    String globalTopic = "global-topic";

    String globalTopicCapital = "global-topic-capital";

    ProducerBuilder<String, Ping> producerPing;

    ProducerBuilder<String, String> producerGlobalTopic;

    ConsumerBuilder<String, Ping> consumer;

    @InjectKafkaCompanion
    KafkaCompanion companion;

    @BeforeEach
    public void setup() {
        consumer = companion.consumeWithDeserializers(new StringDeserializer(),
                new KafkaProtobufDeserializer<>(Ping.parser())).withGroupId("test")
                .withOffsetReset(OffsetResetStrategy.EARLIEST.toString()).withAutoCommit();
        producerPing = companion.produceWithSerializers(new StringSerializer(),
                new KafkaProtobufSerializer<>());
        producerGlobalTopic = companion.produceWithSerializers(new StringSerializer(),
                new StringSerializer());
    }

    @AfterEach
    public void tearDown() {
        producerPing.close();
        producerGlobalTopic.close();
        consumer.close();
    }

    @Test
    void testGlobalStoreValueRetrieval() throws InterruptedException {
        producerPing.fromRecords(new ProducerRecord<>(senderTopic, "ID1", Ping.newBuilder().setMessage("whatever").build()))
                .awaitCompletion(Duration.ofSeconds(1));
        ConsumerRecord<String, Ping> receivedRecord = consumer.fromTopics(consumerTopic, 1)
                .awaitCompletion(Duration.ofSeconds(5)).getFirstRecord();
        assertThat(receivedRecord.value().getMessage(), containsString("Stored value for ID1 is null"));

        // Store two values using the two global topics
        producerGlobalTopic.fromRecords(new ProducerRecord<>(globalTopic, "ID1", "dont-capitalize-me"))
                .awaitCompletion(Duration.ofSeconds(1));
        producerGlobalTopic.fromRecords(new ProducerRecord<>(globalTopicCapital, "ID1", "capitalize-me"))
                .awaitCompletion(Duration.ofSeconds(1));
        Thread.sleep(1000L);
        producerPing.fromRecords(new ProducerRecord<>(senderTopic, "ID1", Ping.newBuilder().setMessage("whatever").build()))
                .awaitCompletion(Duration.ofSeconds(1));
        receivedRecord = consumer.fromTopics(consumerTopic, 1).awaitCompletion(Duration.ofSeconds(5)).getFirstRecord();
        // Check that the value has been stored in the global store
        assertThat(receivedRecord.value().getMessage(),
                containsString("Stored value for ID1 is dont-capitalize-me and capitalized value is CAPITALIZE-ME"));

        // Check that the value still exists in the global store
        producerPing.fromRecords(new ProducerRecord<>(senderTopic, "ID1", Ping.newBuilder().setMessage("whatever").build()))
                .awaitCompletion(Duration.ofSeconds(1));
        Thread.sleep(1000L);
        receivedRecord = consumer.fromTopics(consumerTopic, 1).awaitCompletion(Duration.ofSeconds(5)).getFirstRecord();
        assertThat(receivedRecord.value().getMessage(),
                containsString("Stored value for ID1 is dont-capitalize-me and capitalized value is CAPITALIZE-ME"));

        // Store two new values using the two global topics
        producerGlobalTopic.fromRecords(new ProducerRecord<>(globalTopic, "ID1", "dont-capitalize-me-2"))
                .awaitCompletion(Duration.ofSeconds(1));
        producerGlobalTopic.fromRecords(new ProducerRecord<>(globalTopicCapital, "ID1", "capitalize-me-2"))
                .awaitCompletion(Duration.ofSeconds(1));
        Thread.sleep(1000L);
        producerPing.fromRecords(new ProducerRecord<>(senderTopic, "ID1", Ping.newBuilder().setMessage("whatever").build()))
                .awaitCompletion(Duration.ofSeconds(1));
        receivedRecord = consumer.fromTopics(consumerTopic, 1).awaitCompletion(Duration.ofSeconds(5)).getFirstRecord();
        // Check that the value has been stored in the global store
        assertThat(receivedRecord.value().getMessage(),
                containsString("Stored value for ID1 is dont-capitalize-me-2 and capitalized value is CAPITALIZE-ME-2"));
    }
}
