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
package io.quarkiverse.kafkastreamsprocessor.sample.multioutput;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Properties;

import jakarta.inject.Inject;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.github.daniel.shuy.kafka.protobuf.serde.KafkaProtobufDeserializer;
import com.github.daniel.shuy.kafka.protobuf.serde.KafkaProtobufSerializer;

import io.quarkiverse.kafkastreamsprocessor.sample.message.PingMessage.Ping;
import io.quarkiverse.kafkastreamsprocessor.spi.properties.KStreamsProcessorConfig;
import io.quarkus.test.junit.QuarkusTest;

/**
 * White-box test that runs only in JVM mode.
 */
@QuarkusTest
public class PingProcessorTopologyTest {

    @Inject
    KStreamsProcessorConfig kStreamsProcessorConfig;

    @ConfigProperty(name = "kafkastreamsprocessor.output.sinks.pong.topic")
    String pongTopic;

    @ConfigProperty(name = "kafkastreamsprocessor.output.sinks.pang.topic")
    String pangTopic;

    TopologyTestDriver testDriver;

    @Inject
    Topology topology;

    TestInputTopic<String, Ping> testInputTopic;

    TestOutputTopic<String, Ping> testPongTopic;

    TestOutputTopic<String, Ping> testPangTopic;

    @BeforeEach
    public void setup() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test.id");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        testDriver = new TopologyTestDriver(topology, config);

        testInputTopic = testDriver.createInputTopic(kStreamsProcessorConfig.input().topic().get(),
                new StringSerializer(),
                new KafkaProtobufSerializer<>());
        testPongTopic = testDriver.createOutputTopic(pongTopic, new StringDeserializer(),
                new KafkaProtobufDeserializer<>(Ping.parser()));
        testPangTopic = testDriver.createOutputTopic(pangTopic, new StringDeserializer(),
                new KafkaProtobufDeserializer<>(Ping.parser()));
    }

    @AfterEach
    public void tearDown() {
        testDriver.close();
    }

    @Test
    public void topologyShouldOutputOKMessage() {
        Ping inputPing = Ping.newBuilder().setMessage("world").build();
        testInputTopic.pipeInput(inputPing);
        TestRecord<String, Ping> result = testPongTopic.readRecord();
        assertEquals("5", result.value().getMessage());
    }

    @Test
    public void topologyShouldOutputKOMessage() {
        Ping inputPing = Ping.newBuilder().setMessage("error").build();
        testInputTopic.pipeInput(inputPing);
        TestRecord<String, Ping> result = testPangTopic.readRecord();
        assertEquals("PANG!", result.value().getMessage());
    }
}
