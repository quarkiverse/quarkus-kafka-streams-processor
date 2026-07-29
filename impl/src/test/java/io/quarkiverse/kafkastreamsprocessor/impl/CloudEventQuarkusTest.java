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

import static io.quarkiverse.kafkastreamsprocessor.impl.protocol.KafkaStreamsProcessorHeaders.DLQ_CAUSE;
import static io.quarkiverse.kafkastreamsprocessor.impl.protocol.KafkaStreamsProcessorHeaders.DLQ_PARTITION;
import static io.quarkiverse.kafkastreamsprocessor.impl.protocol.KafkaStreamsProcessorHeaders.DLQ_REASON;
import static io.quarkiverse.kafkastreamsprocessor.impl.protocol.KafkaStreamsProcessorHeaders.DLQ_TOPIC;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import jakarta.enterprise.inject.Alternative;
import jakarta.inject.Inject;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.v1.CloudEventBuilder;
import io.cloudevents.kafka.CloudEventDeserializer;
import io.cloudevents.kafka.CloudEventSerializer;
import io.quarkiverse.kafkastreamsprocessor.api.Processor;
import io.quarkiverse.kafkastreamsprocessor.api.cloudevents.CloudEventContextHandler;
import io.quarkiverse.kafkastreamsprocessor.sample.message.PingMessage;
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
@TestProfile(CloudEventQuarkusTest.TestProfile.class)
@QuarkusTestResource(KafkaCompanionResource.class)
public class CloudEventQuarkusTest {
    private static final String DLQ_TOPIC_NAME = "dlq-topic";
    private static final String PROCESS_AND_FAIL_MESSAGE = "Process&Fail";

    @ConfigProperty(name = "kafkastreamsprocessor.input.topic")
    String senderTopic;

    @ConfigProperty(name = "kafkastreamsprocessor.output.topic")
    String consumerTopic;

    @InjectKafkaCompanion
    KafkaCompanion companion;

    ProducerBuilder<String, CloudEvent> producer;

    ConsumerBuilder<String, CloudEvent> consumer;

    @BeforeEach
    public void setup() {
        producer = companion.produceWithSerializers(new StringSerializer(), new CloudEventSerializer());
        consumer = companion.consumeWithDeserializers(new StringDeserializer(), new CloudEventDeserializer())
                .withGroupId("test").withOffsetReset(OffsetResetStrategy.EARLIEST.toString()).withAutoCommit();
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

        producer.fromRecords(sentRecord).awaitCompletion(Duration.ofSeconds(5));

        ConsumerRecord<String, CloudEvent> singleRecord = consumer.fromTopics(consumerTopic, 1)
                .awaitCompletion(Duration.ofSeconds(10)).getFirstRecord();

        assertThat(singleRecord.key(), equalTo("key"));
        assertThat(PingMessage.Ping.parseFrom(singleRecord.value().getData().toBytes()).getMessage(), equalTo("blabla"));
        System.out.println(singleRecord.headers());
        assertThat(singleRecord.value().getType(), equalTo("mirrored-string-message"));
        assertThat(singleRecord.value().getId(), not(equalTo(cloudEvent.getId())));
        assertThat(singleRecord.value().getSource().toString(), equalTo("my-test-processor"));
        assertThat(singleRecord.value().getExtensionNames(), contains("someextension"));
        assertThat(singleRecord.value().getTime(), notNullValue());
    }

    @Test
    public void cloudEventsProcessingErrorsShouldGoInTheDLQ() throws Exception {
        CloudEvent cloudEvent = new CloudEventBuilder()
                .withData(PingMessage.Ping.newBuilder().setMessage(PROCESS_AND_FAIL_MESSAGE).build().toByteArray())
                .withType("string-message")
                .withId(UUID.randomUUID().toString())
                .withSource(URI.create("blabla"))
                .build();
        ProducerRecord<String, CloudEvent> sentRecord = new ProducerRecord<>(senderTopic, 0, "key", cloudEvent);

        producer.fromRecords(sentRecord).awaitCompletion(Duration.ofSeconds(5));

        ConsumerRecord<String, CloudEvent> dlqRecord = consumer.fromTopics(DLQ_TOPIC_NAME, 1)
                .awaitCompletion(Duration.ofSeconds(10)).getFirstRecord();

        assertThat(dlqRecord.key(), equalTo("key"));
        assertThat(PingMessage.Ping.parseFrom(dlqRecord.value().getData().toBytes()).getMessage(),
                equalTo(PROCESS_AND_FAIL_MESSAGE));
        System.out.println(dlqRecord.headers());
        assertThat(dlqRecord.value().getType(), equalTo(cloudEvent.getType()));
        assertThat(dlqRecord.value().getId(), equalTo(cloudEvent.getId()));
        assertThat(dlqRecord.value().getSource(), equalTo(cloudEvent.getSource()));
        assertThat(headerValue(dlqRecord, DLQ_REASON), equalTo("Processor code throwing exception"));
        assertThat(headerValue(dlqRecord, DLQ_CAUSE), equalTo("java.lang.Throwable"));
        assertThat(headerValue(dlqRecord, DLQ_PARTITION), equalTo("0"));
        assertThat(headerValue(dlqRecord, DLQ_TOPIC), equalTo(senderTopic));
    }

    @Processor
    @Alternative
    @Slf4j
    public static class TestProcessor extends ContextualProcessor<String, PingMessage.Ping, String, PingMessage.Ping> {
        @Inject
        CloudEventContextHandler cloudEventContextHandler;

        @Override
        public void process(Record<String, PingMessage.Ping> record) {
            if (record.value().getMessage().equals(PROCESS_AND_FAIL_MESSAGE)) {
                throw new RuntimeException("Processor code throwing exception", new Throwable());
            }

            assertThat(cloudEventContextHandler.getIncomingContext().getSource().toString(), equalTo("blabla"));
            assertThat(cloudEventContextHandler.getIncomingContext().getType(), equalTo("string-message"));
            assertThat(cloudEventContextHandler.getIncomingContext().getId(), notNullValue());
            cloudEventContextHandler.setOutgoingContext(
                    cloudEventContextHandler.contextBuilder().withExtension("someextension", "blabla").build());
            context().forward(record);
        }
    }

    private String headerValue(ConsumerRecord<?, ?> record, String headerName) {
        return new String(record.headers().lastHeader(headerName).value(), StandardCharsets.UTF_8);
    }

    public static class TestProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of("kafkastreamsprocessor.input.is-cloud-event", "true", "kafkastreamsprocessor.output.is-cloud-event",
                    "true", "kafkastreamsprocessor.output.cloud-events-type", "mirrored-string-message",
                    "kafkastreamsprocessor.output.cloud-events-source", "my-test-processor",
                    "kafkastreamsprocessor.error-strategy", "dead-letter-queue",
                    "kafkastreamsprocessor.dlq.topic", DLQ_TOPIC_NAME);
        }

        @Override
        public Set<Class<?>> getEnabledAlternatives() {
            return Set.of(TestProcessor.class);
        }
    }
}
