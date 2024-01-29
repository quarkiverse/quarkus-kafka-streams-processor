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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.quarkiverse.kafkastreamsprocessor.spi.properties.KStreamsProcessorConfig;
import io.quarkus.test.junit.QuarkusTest;

@QuarkusTest
public class PojoProcessorTopologyTest {

    @Inject
    KStreamsProcessorConfig kStreamsProcessorConfig;

    TopologyTestDriver testDriver;

    @Inject
    Topology topology;

    TestInputTopic<String, String> testInputTopic;

    TestOutputTopic<String, String> testOutputTopic;

    @BeforeEach
    public void setup() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test.id");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        testDriver = new TopologyTestDriver(topology, config);

        testInputTopic = testDriver.createInputTopic(kStreamsProcessorConfig.input().topic().get(),
                new StringSerializer(),
                new StringSerializer());
        testOutputTopic = testDriver.createOutputTopic(kStreamsProcessorConfig.output().topic().get(),
                new StringDeserializer(),
                new StringDeserializer());
    }

    @AfterEach
    public void tearDown() {
        testDriver.close();
    }

    @Test
    public void topologyShouldOutputMessageInCamelCase() {
        String input = "{\"string_field\":\"hello\",\"numerical_field\":1234,\"boolean_field\":true}";
        testInputTopic.pipeInput(input);
        assertEquals("{\"string_field\":\"olleh\",\"numerical_field\":1271,\"boolean_field\":false}",
                testOutputTopic.readRecord().value());
    }
}
