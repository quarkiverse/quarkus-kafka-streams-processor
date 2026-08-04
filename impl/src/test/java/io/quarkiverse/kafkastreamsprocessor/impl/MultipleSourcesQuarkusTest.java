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
package io.quarkiverse.kafkastreamsprocessor.impl;

import static io.quarkiverse.kafkastreamsprocessor.sample.message.PingMessage.Ping;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;

import jakarta.enterprise.inject.Alternative;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.github.daniel.shuy.kafka.protobuf.serde.KafkaProtobufDeserializer;
import com.github.daniel.shuy.kafka.protobuf.serde.KafkaProtobufSerializer;

import io.quarkiverse.kafkastreamsprocessor.api.Processor;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.kafka.InjectKafkaCompanion;
import io.quarkus.test.kafka.KafkaCompanionResource;
import io.smallrye.reactive.messaging.kafka.companion.ConsumerBuilder;
import io.smallrye.reactive.messaging.kafka.companion.KafkaCompanion;
import io.smallrye.reactive.messaging.kafka.companion.ProducerBuilder;
import lombok.extern.slf4j.Slf4j;

@QuarkusTest
@TestProfile(MultipleSourcesQuarkusTest.TestProfile.class)
@Slf4j
@QuarkusTestResource(KafkaCompanionResource.class)
public class MultipleSourcesQuarkusTest {
    // @ConfigProperty can't be used as it will clash with other
    // test cases (the annotations are checked even if no test cases
    // are run)
    String[] pingTopics = new String[] { "ping-topic", "other-ping" };
    String[] pangTopics = new String[] { "pang-topic" };
    String pongTopic = "pong-topic";

    @InjectKafkaCompanion
    KafkaCompanion companion;

    ProducerBuilder<String, Ping> producer;

    ConsumerBuilder<String, Ping> consumer;

    @BeforeEach
    public void setup() {
        producer = companion.produceWithSerializers(new StringSerializer(), new KafkaProtobufSerializer<>());
        consumer = companion.consumeWithDeserializers(new StringDeserializer(), new KafkaProtobufDeserializer<>(Ping.parser()))
                .withGroupId("test").withOffsetReset(OffsetResetStrategy.EARLIEST.toString()).withAutoCommit();
    }

    @AfterEach
    public void tearDown() throws Exception {
        producer.close();
        consumer.close();
    }

    @Test
    public void whenProducingFromEachInputTopic_shouldProduce() {

        // use topic(s) in List.of(pongTopic) with consumer.fromTopics at consumption time

        Ping ping = Ping.newBuilder().setMessage("world").build();

        producer.fromRecords(new ProducerRecord<>(pingTopics[0], null, ping)).awaitCompletion(Duration.ofSeconds(1));
        producer.fromRecords(new ProducerRecord<>(pingTopics[1], null, ping)).awaitCompletion(Duration.ofSeconds(1));
        producer.fromRecords(new ProducerRecord<>(pangTopics[0], null, ping)).awaitCompletion(Duration.ofSeconds(1));

        List<ConsumerRecord<String, Ping>> records = consumer.fromTopics(pongTopic, 3).awaitCompletion(Duration.ofSeconds(10))
                .getRecords();

        assertThat(records, hasSize(3));
    }

    @Processor
    @Alternative
    public static class TestProcessor extends ContextualProcessor<String, Ping, String, Ping> {

        @Override
        public void process(Record<String, Ping> record) {
            context().forward(record);
        }
    }

    public static class TestProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of(
                    "kafkastreamsprocessor.input.sources.ping.topics", "ping-topic,other-ping",
                    "kafkastreamsprocessor.input.sources.pang.topics", "pang-topic",
                    "kafkastreamsprocessor.output.sinks.pong.topic", "pong-topic",
                    "quarkus.kafka-streams.topics", "ping-topic,other-ping,pang-topic,pong-topic");
        }

        @Override
        public Set<Class<?>> getEnabledAlternatives() {
            return Set.of(TestProcessor.class);
        }
    }
}
