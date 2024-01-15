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

import jakarta.enterprise.context.Dependent;
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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.quarkiverse.kafkastreamsprocessor.api.Processor;
import io.quarkiverse.kafkastreamsprocessor.api.configuration.Configuration;
import io.quarkiverse.kafkastreamsprocessor.api.configuration.ConfigurationCustomizer;
import io.quarkiverse.kafkastreamsprocessor.api.properties.KStreamsProcessorConfig;
import io.quarkiverse.kafkastreamsprocessor.api.serdes.JacksonSerde;
import io.quarkiverse.kafkastreamsprocessor.api.serdes.JacksonSerializer;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@QuarkusTest
@TestProfile(CustomTopologyWithPojoDriverTest.SimpleProcessorProfile.class)
public class CustomTopologyWithPojoDriverTest {
    @Inject
    Topology topology;

    TopologyTestDriver testDriver;

    @Inject
    KStreamsProcessorConfig kStreamsProcessorConfig;

    TestInputTopic<String, String> testInputTopic;

    TestOutputTopic<String, String> testOutputTopic;

    @BeforeEach
    public void setup() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        testDriver = new TopologyTestDriver(topology, config);
        testInputTopic = testDriver.createInputTopic(kStreamsProcessorConfig.input().topic().get(),
                new StringSerializer(), new StringSerializer());
        testOutputTopic = testDriver.createOutputTopic(kStreamsProcessorConfig.output().topic().get(),
                new StringDeserializer(),
                new StringDeserializer());
    }

    @Test
    public void topologyShouldOutputReversedMessage() {
        String input = "{\"stringField\":\"hello\",\"numericalField\":1234,\"booleanField\":true}";
        testInputTopic.pipeInput(input);
        assertEquals("{\"stringField\":\"olleh\",\"numericalField\":1271,\"booleanField\":false}",
                testOutputTopic.readRecord().value());
    }

    @Processor
    @Alternative
    @Slf4j
    public static class TestProcessor extends ContextualProcessor<String, MyPojo, String, MyPojo> {

        @Override
        public void process(Record<String, MyPojo> record) {
            String reversedMsg = new StringBuilder(record.value().getStringField()).reverse().toString();
            log.info("Received value {} sending back {} in response", record.value().getStringField(), reversedMsg);
            MyPojo pojo = new MyPojo(reversedMsg, record.value().getNumericalField() + 37, !record.value().getBooleanField());
            context().forward(record.withValue(pojo));
        }
    }

    @Alternative
    @Dependent
    public static class MyConfigCustomizer implements ConfigurationCustomizer {
        private static final ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public void fillConfiguration(Configuration configuration) {
            configuration.setSourceValueSerde(new JacksonSerde(configuration.getProcessorPayloadType(), objectMapper));
            configuration.setSinkValueSerializer(new JacksonSerializer<>(objectMapper));
        }
    }

    public static class SimpleProcessorProfile implements QuarkusTestProfile {
        @Override
        public Set<Class<?>> getEnabledAlternatives() {
            return Set.of(TestProcessor.class, MyConfigCustomizer.class);
        }
    }

    @NoArgsConstructor
    @AllArgsConstructor
    @Data
    public static class MyPojo {

        @JsonProperty()
        private String stringField;

        @JsonProperty()
        private Integer numericalField;

        @JsonProperty()
        private Boolean booleanField;

    }
}
