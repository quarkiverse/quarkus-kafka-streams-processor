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
package io.quarkiverse.kafkastreamsprocessor.sample.jsonpojo;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.time.Duration;

import jakarta.inject.Inject;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Durations;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.quarkiverse.kafkastreamsprocessor.spi.properties.KStreamsProcessorConfig;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.kafka.InjectKafkaCompanion;
import io.quarkus.test.kafka.KafkaCompanionResource;
import io.smallrye.reactive.messaging.kafka.companion.ConsumerBuilder;
import io.smallrye.reactive.messaging.kafka.companion.KafkaCompanion;
import io.smallrye.reactive.messaging.kafka.companion.ProducerBuilder;

@QuarkusTest
@QuarkusTestResource(KafkaCompanionResource.class)
public class PojoProcessorQuarkusTest {
    @InjectKafkaCompanion
    KafkaCompanion companion;

    @Inject
    KStreamsProcessorConfig kStreamsProcessorConfig;

    ProducerBuilder<String, String> producer;

    ConsumerBuilder<String, String> consumer;

    @Inject
    ObjectMapper objectMapper;

    @BeforeEach
    public void setup() {
        consumer = companion.consumeWithDeserializers(new StringDeserializer(), new StringDeserializer())
                .withGroupId("test").withOffsetReset(OffsetResetStrategy.EARLIEST.toString()).withAutoCommit();
        producer = companion.produceWithSerializers(new StringSerializer(), new StringSerializer());
    }

    @AfterEach
    public void tearDown() {
        producer.close();
        consumer.close();
    }

    @Test
    public void processorShouldHandleJsonString() throws JsonProcessingException {
        SamplePojo pojo = new SamplePojo("hello", 1234, true);
        String json = objectMapper.writeValueAsString(pojo);

        producer.fromRecords(new ProducerRecord<>(kStreamsProcessorConfig.input().topic().get(), json))
                .awaitCompletion(Duration.ofSeconds(1));

        ConsumerRecord<String, String> record = consumer.fromTopics(kStreamsProcessorConfig.output().topic().get(), 1)
                .awaitCompletion(Durations.FIVE_SECONDS)
                .getFirstRecord();
        SamplePojo expected = new SamplePojo("olleh", 1271, false);

        assertThat(objectMapper.readValue(record.value(), SamplePojo.class), is(equalTo(expected)));
    }
}
