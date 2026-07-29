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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.time.Duration;
import java.util.Map;
import java.util.Set;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.inject.Inject;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.opentelemetry.api.trace.Tracer;
import io.quarkiverse.kafkastreamsprocessor.api.Processor;
import io.quarkiverse.kafkastreamsprocessor.api.configuration.Configuration;
import io.quarkiverse.kafkastreamsprocessor.api.configuration.ConfigurationCustomizer;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.kafka.InjectKafkaCompanion;
import io.quarkus.test.kafka.KafkaCompanionResource;
import io.smallrye.reactive.messaging.kafka.companion.ConsumerBuilder;
import io.smallrye.reactive.messaging.kafka.companion.KafkaCompanion;
import io.smallrye.reactive.messaging.kafka.companion.ProducerBuilder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@QuarkusTest
@TestProfile(CustomKeyTypeSupportedQuarkusTest.TestProfile.class)
@QuarkusTestResource(KafkaCompanionResource.class)
public class CustomKeyTypeSupportedQuarkusTest {
    @ConfigProperty(name = "kafkastreamsprocessor.input.topic")
    String senderTopic;

    @ConfigProperty(name = "kafkastreamsprocessor.output.topic")
    String consumerTopic;

    @InjectKafkaCompanion
    KafkaCompanion companion;

    ProducerBuilder<KeyType, String> producer;

    ConsumerBuilder<KeyType, String> consumer;

    @BeforeEach
    public void setup() {
        producer = companion.produceWithSerializers(new KeySerializer(), new StringSerializer());
        consumer = companion.consumeWithDeserializers(new KeyDeserializer(), new StringDeserializer())
                .withGroupId("test").withOffsetReset(OffsetResetStrategy.EARLIEST.toString()).withAutoCommit();
    }

    @AfterEach
    public void tearDown() throws Exception {
        producer.close();
        consumer.close();
    }

    @Test
    public void customKeyTypeSupported() {
        ProducerRecord<KeyType, String> sentRecord = new ProducerRecord<>(senderTopic, 0, new KeyType("key"), "value");

        producer.fromRecords(sentRecord).awaitCompletion(Duration.ofSeconds(5));

        ConsumerRecord<KeyType, String> singleRecord = consumer.fromTopics(consumerTopic, 1)
                .awaitCompletion(Duration.ofSeconds(10)).getFirstRecord();

        assertThat(singleRecord.key().getKey(), equalTo("key$"));
        assertThat(singleRecord.value(), equalTo("valueg"));
    }

    @Data
    public static class KeyType {
        private final String key;
    }

    public static class KeySerializer implements Serializer<KeyType> {
        private final StringSerializer serializer = new StringSerializer();

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            serializer.configure(configs, isKey);
        }

        @Override
        public byte[] serialize(String topic, KeyType keyType) {
            return serializer.serialize(topic, keyType.getKey());
        }
    }

    public static class KeyDeserializer implements Deserializer<KeyType> {
        private final StringDeserializer deserializer = new StringDeserializer();

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            deserializer.configure(configs, isKey);
        }

        @Override
        public KeyType deserialize(String topic, byte[] data) {
            String key = deserializer.deserialize(topic, data);
            return new KeyType(key);
        }
    }

    public static class KeySerde implements Serde<KeyType> {
        @Override
        public Serializer<KeyType> serializer() {
            return new KeySerializer();
        }

        @Override
        public Deserializer<KeyType> deserializer() {
            return new KeyDeserializer();
        }
    }

    @Processor
    @Alternative
    @Slf4j
    public static class TestProcessor extends ContextualProcessor<KeyType, String, KeyType, String> {
        @Inject
        Tracer tracer;

        @Override
        public void process(Record<KeyType, String> record) {
            record = record.withValue(record.value() + "g");
            record = record.withKey(new KeyType(record.key().getKey() + "$"));

            context().forward(record);
        }
    }

    @ApplicationScoped
    @Alternative
    public static class MyConfiguration implements ConfigurationCustomizer {
        @Override
        public void fillConfiguration(Configuration configuration) {
            configuration.setSourceKeySerde(new KeySerde());
            configuration.setSourceValueSerde(Serdes.String());
            configuration.setSinkKeySerializer(new KeySerializer());
            configuration.setSinkValueSerializer(new StringSerializer());
        }
    }

    public static class TestProfile implements QuarkusTestProfile {
        @Override
        public Set<Class<?>> getEnabledAlternatives() {
            return Set.of(TestProcessor.class, MyConfiguration.class);
        }
    }
}
