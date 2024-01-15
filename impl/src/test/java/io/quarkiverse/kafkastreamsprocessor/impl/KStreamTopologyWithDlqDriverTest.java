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

import static io.quarkiverse.kafkastreamsprocessor.sample.message.PingMessage.Ping.newBuilder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

import jakarta.enterprise.inject.Alternative;
import jakarta.inject.Inject;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.github.daniel.shuy.kafka.protobuf.serde.KafkaProtobufDeserializer;
import com.github.daniel.shuy.kafka.protobuf.serde.KafkaProtobufSerializer;

import io.quarkiverse.kafkastreamsprocessor.api.Processor;
import io.quarkiverse.kafkastreamsprocessor.api.properties.KStreamsProcessorConfig;
import io.quarkiverse.kafkastreamsprocessor.sample.message.PingMessage;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import lombok.extern.slf4j.Slf4j;

@QuarkusTest
@TestProfile(KStreamTopologyWithDlqDriverTest.SimpleProtobufProcessorProfile.class)
public class KStreamTopologyWithDlqDriverTest {

    @Inject
    Topology topology;

    TopologyTestDriver testDriver;

    @Inject
    KStreamsProcessorConfig kStreamsProcessorConfig;

    TestInputTopic<String, PingMessage.Ping> testInputTopic;

    TestOutputTopic<String, PingMessage.Ping> testOutputTopic;

    TestOutputTopic<String, PingMessage.Ping> testDlqOutputTopic;

    @BeforeEach
    public void setup() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        testDriver = new TopologyTestDriver(topology, config);
        testInputTopic = testDriver.createInputTopic(kStreamsProcessorConfig.input().topic().get(), new StringSerializer(),
                new KafkaProtobufSerializer<PingMessage.Ping>());
        testOutputTopic = testDriver.createOutputTopic(kStreamsProcessorConfig.output().topic().get(),
                new StringDeserializer(),
                new KafkaProtobufDeserializer<>(PingMessage.Ping.parser()));
        if (kStreamsProcessorConfig.dlq().topic().isPresent()) {
            testDlqOutputTopic = testDriver.createOutputTopic(kStreamsProcessorConfig.dlq().topic().get(),
                    new StringDeserializer(),
                    new KafkaProtobufDeserializer<>(PingMessage.Ping.parser()));
        }
    }

    @Test
    public void processorExceptionShouldSendToDlq() {
        PingMessage.Ping input = newBuilder().setMessage("null").build();
        testInputTopic.pipeInput(input);
        assertEquals("null", testDlqOutputTopic.readRecord().value().getMessage());
        assertTrue(testOutputTopic.isEmpty());
    }

    @Test
    public void processorWithoutExceptionShouldNotSendToDlq() {
        PingMessage.Ping input = newBuilder().setMessage("ping").build();
        testInputTopic.pipeInput(input);
        assertEquals("ping", testOutputTopic.readRecord().value().getMessage());
        assertTrue(testDlqOutputTopic.isEmpty());
    }

    @Processor
    @Alternative
    @Slf4j
    public static class TestProcessor extends ContextualProcessor<String, PingMessage.Ping, String, PingMessage.Ping> {

        @Override
        public void process(Record<String, PingMessage.Ping> record) {
            log.info("Processing...");
            if ("null".equals(record.value().getMessage())) {
                throw new TestException();
            }
            context().forward(record);
        }
    }

    public static class SimpleProtobufProcessorProfile implements QuarkusTestProfile {

        @Override
        public Set<Class<?>> getEnabledAlternatives() {
            return Set.of(TestProcessor.class);
        }

        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of("kafkastreamsprocessor.error-strategy", "dead-letter-queue",
                    "kafkastreamsprocessor.dlq.topic", "dlq");
        }
    }
}
