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

import static org.junit.jupiter.api.Assertions.assertEquals;

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

import io.quarkiverse.kafkastreamsprocessor.api.Processor;
import io.quarkiverse.kafkastreamsprocessor.api.properties.KStreamsProcessorConfig;
import io.quarkiverse.kafkastreamsprocessor.api.serdes.JacksonDeserializer;
import io.quarkiverse.kafkastreamsprocessor.api.serdes.JacksonSerializer;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@QuarkusTest
@TestProfile(JsonPayloadTopologyDriverTest.JsonProcessorProfile.class)
class JsonPayloadTopologyDriverTest {

    @Inject
    Topology topology;

    TopologyTestDriver testDriver;

    @Inject
    KStreamsProcessorConfig kStreamsProcessorConfig;

    TestInputTopic<String, JSonPojo> testInputTopic;

    TestOutputTopic<String, JSonPojo> testOutputTopic;

    @BeforeEach
    public void setup() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        testDriver = new TopologyTestDriver(topology, config);
        testInputTopic = testDriver.createInputTopic(kStreamsProcessorConfig.input().topic().get(),
                new StringSerializer(),
                new JacksonSerializer<>());
        testOutputTopic = testDriver.createOutputTopic(kStreamsProcessorConfig.output().topic().get(),
                new StringDeserializer(),
                new JacksonDeserializer<>(JSonPojo.class));
    }

    @Test
    void topologyShouldOutputMessage() {
        JSonPojo pojo = new JSonPojo("message");
        testInputTopic.pipeInput(pojo);
        JSonPojo processedPojo = testOutputTopic.readRecord().getValue();
        assertEquals("processed message", processedPojo.message);
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class JSonPojo {
        String message;
    }

    @Processor
    @Alternative
    public static class JsonProcessor extends ContextualProcessor<String, JSonPojo, String, JSonPojo> {

        @Override
        public void process(Record<String, JSonPojo> record) {
            JSonPojo pojo = record.value();
            record = record.withValue(new JSonPojo("processed " + pojo.message));
            context().forward(record);
        }
    }

    public static class JsonProcessorProfile implements QuarkusTestProfile {
        @Override
        public Set<Class<?>> getEnabledAlternatives() {
            return Set.of(JsonProcessor.class);
        }
    }
}
