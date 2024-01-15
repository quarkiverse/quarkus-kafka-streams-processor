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
package io.quarkiverse.kafkastreamsprocessor.sample.simple;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.github.daniel.shuy.kafka.protobuf.serde.KafkaProtobufDeserializer;
import com.github.daniel.shuy.kafka.protobuf.serde.KafkaProtobufSerializer;

import io.quarkiverse.kafkastreamsprocessor.api.properties.KStreamsProcessorConfig;
import io.quarkiverse.kafkastreamsprocessor.sample.message.PingMessage.Ping;
import io.quarkus.test.junit.QuarkusTest;

@QuarkusTest
public class PingProcessorTopologyTest {

    @Inject
    KStreamsProcessorConfig kStreamsProcessorConfig;

    TopologyTestDriver testDriver;

    @Inject
    Topology topology;

    TestInputTopic<String, Ping> testInputTopic;

    TestOutputTopic<String, Ping> testOutputTopic;

    @BeforeEach
    public void setup() {

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test.id");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        testDriver = new TopologyTestDriver(topology, config);

        testInputTopic = testDriver.createInputTopic(kStreamsProcessorConfig.input().topic().get(),
                new StringSerializer(),
                new KafkaProtobufSerializer<>());
        testOutputTopic = testDriver.createOutputTopic(kStreamsProcessorConfig.output().topic().get(),
                new StringDeserializer(),
                new KafkaProtobufDeserializer<>(Ping.parser()));
    }

    @AfterEach
    public void tearDown() {
        testDriver.close();
    }

    @Test
    public void topologyShouldOutputMessage() {
        Ping inputPing = Ping.newBuilder().setMessage("world").build();

        testInputTopic.pipeInput(inputPing);
        TestRecord<String, Ping> result = testOutputTopic.readRecord();
        assertEquals("5", result.value().getMessage());
    }

    @Test
    public void topologyShouldGenerateMessage() {
        Ping inputPing = Ping.newBuilder().setMessage("bigMessage-152").build();

        testInputTopic.pipeInput(inputPing);
        TestRecord<String, Ping> result = testOutputTopic.readRecord();
        assertEquals(152, result.value().getMessage().getBytes(StandardCharsets.UTF_8).length);
    }

    @Test
    public void topologyShouldEndInException() {
        Ping inputPing = Ping.newBuilder().setMessage("error").build();

        testInputTopic.pipeInput(inputPing);
        assertThrows(NoSuchElementException.class, () -> {
            testOutputTopic.readRecord();
        });
    }
}
