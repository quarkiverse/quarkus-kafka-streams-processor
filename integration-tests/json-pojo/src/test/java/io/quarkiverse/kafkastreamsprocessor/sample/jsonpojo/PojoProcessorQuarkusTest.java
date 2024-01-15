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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.quarkiverse.kafkastreamsprocessor.api.properties.KStreamsProcessorConfig;
import io.quarkus.test.junit.QuarkusTest;

@QuarkusTest
public class PojoProcessorQuarkusTest {

    @ConfigProperty(name = "kafka.bootstrap.servers")
    String kafkaBootstrapServers;

    @Inject
    KStreamsProcessorConfig kStreamsProcessorConfig;

    KafkaProducer<String, String> producer;

    KafkaConsumer<String, String> consumer;

    @Inject
    ObjectMapper objectMapper;

    @BeforeEach
    public void setup() {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(kafkaBootstrapServers, "test", "true");
        consumer = new KafkaConsumer<>(consumerProps, new StringDeserializer(),
                new StringDeserializer());
        consumer.subscribe(List.of(kStreamsProcessorConfig.output().topic().get()));
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(kafkaBootstrapServers);
        producer = new KafkaProducer<>(producerProps, new StringSerializer(), new StringSerializer());
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

        producer.send(new ProducerRecord<>(kStreamsProcessorConfig.input().topic().get(), json));
        producer.flush();

        ConsumerRecord<String, String> record = KafkaTestUtils.getSingleRecord(consumer,
                kStreamsProcessorConfig.output().topic().get(), Durations.FIVE_SECONDS);
        SamplePojo expected = new SamplePojo("olleh", 1271, false);

        assertThat(objectMapper.readValue(record.value(), SamplePojo.class), is(equalTo(expected)));
    }
}
