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
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import jakarta.enterprise.inject.Alternative;
import jakarta.inject.Inject;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.v1.CloudEventBuilder;
import io.cloudevents.kafka.CloudEventDeserializer;
import io.cloudevents.kafka.CloudEventSerializer;
import io.quarkiverse.kafkastreamsprocessor.api.Processor;
import io.quarkiverse.kafkastreamsprocessor.api.cloudevents.CloudEventContextHandler;
import io.quarkiverse.kafkastreamsprocessor.sample.message.PingMessage;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import lombok.extern.slf4j.Slf4j;

@QuarkusTest
@TestProfile(CloudEventQuarkusTest.TestProfile.class)
public class CloudEventQuarkusTest {
    @ConfigProperty(name = "kafkastreamsprocessor.input.topic")
    String senderTopic;

    @ConfigProperty(name = "kafkastreamsprocessor.output.topic")
    String consumerTopic;

    @ConfigProperty(name = "kafka.bootstrap.servers")
    String bootstrapServers;

    KafkaProducer<String, CloudEvent> producer;

    KafkaConsumer<String, CloudEvent> consumer;

    @BeforeEach
    public void setup() {
        producer = new KafkaProducer<>(KafkaTestUtils.producerProps(bootstrapServers), new StringSerializer(),
                new CloudEventSerializer());
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(bootstrapServers, "test", "true");
        consumer = new KafkaConsumer<>(consumerProps, new StringDeserializer(), new CloudEventDeserializer());
        consumer.subscribe(List.of(consumerTopic));
    }

    @AfterEach
    public void tearDown() throws Exception {
        producer.close();
        consumer.close();
    }

    @Test
    public void exchangeCloudEvents() throws Exception {
        CloudEvent cloudEvent = new CloudEventBuilder()
                .withData(PingMessage.Ping.newBuilder().setMessage("blabla").build().toByteArray())
                .withType("string-message")
                .withId(UUID.randomUUID().toString())
                .withSource(URI.create("blabla"))
                .build();
        ProducerRecord<String, CloudEvent> sentRecord = new ProducerRecord<>(senderTopic, 0, "key", cloudEvent);

        producer.send(sentRecord);
        producer.flush();

        ConsumerRecord<String, CloudEvent> singleRecord = KafkaTestUtils.getSingleRecord(consumer, consumerTopic,
                Duration.ofSeconds(10));

        assertThat(singleRecord.key(), equalTo("key"));
        assertThat(PingMessage.Ping.parseFrom(singleRecord.value().getData().toBytes()).getMessage(), equalTo("blabla"));
        System.out.println(singleRecord.headers());
        assertThat(singleRecord.value().getType(), equalTo("mirrored-string-message"));
        assertThat(singleRecord.value().getId(), not(equalTo(cloudEvent.getId())));
        assertThat(singleRecord.value().getSource().toString(), equalTo("my-test-processor"));
        assertThat(singleRecord.value().getExtensionNames(), contains("someextension"));
    }

    @Processor
    @Alternative
    @Slf4j
    public static class TestProcessor extends ContextualProcessor<String, PingMessage.Ping, String, PingMessage.Ping> {
        @Inject
        CloudEventContextHandler cloudEventContextHandler;

        @Override
        public void process(Record<String, PingMessage.Ping> record) {
            assertThat(cloudEventContextHandler.getIncomingContext().getSource().toString(), equalTo("blabla"));
            assertThat(cloudEventContextHandler.getIncomingContext().getType(), equalTo("string-message"));
            assertThat(cloudEventContextHandler.getIncomingContext().getId(), notNullValue());
            cloudEventContextHandler.setOutgoingContext(
                    cloudEventContextHandler.contextBuilder().withExtension("someextension", "blabla").build());
            context().forward(record);
        }
    }

    public static class TestProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of("kafkastreamsprocessor.input.is-cloud-event", "true", "kafkastreamsprocessor.output.is-cloud-event",
                    "true", "kafkastreamsprocessor.output.cloud-events-type", "mirrored-string-message",
                    "kafkastreamsprocessor.output.cloud-events-source", "my-test-processor");
        }

        @Override
        public Set<Class<?>> getEnabledAlternatives() {
            return Set.of(TestProcessor.class);
        }
    }
}
