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
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
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
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.github.daniel.shuy.kafka.protobuf.serde.KafkaProtobufDeserializer;
import com.github.daniel.shuy.kafka.protobuf.serde.KafkaProtobufSerializer;

import io.quarkiverse.kafkastreamsprocessor.api.Processor;
import io.quarkiverse.kafkastreamsprocessor.api.properties.KStreamsProcessorConfig;
import io.quarkiverse.kafkastreamsprocessor.impl.metrics.KafkaStreamsProcessorMetrics;
import io.quarkiverse.kafkastreamsprocessor.sample.message.PingMessage;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.restassured.http.ContentType;

@QuarkusTest
@TestProfile(KStreamMetricsQuarkusTest.SimpleProtobufProcessorProfile.class)
public class KStreamMetricsQuarkusTest {
    private static boolean punctuatorThrowsException = false;

    @Inject
    KStreamsProcessorConfig kStreamsProcessorConfig;

    TopologyTestDriver testDriver;

    @Inject
    Topology topology;

    TestInputTopic<String, PingMessage.Ping> testInputTopic;

    TestOutputTopic<String, PingMessage.Ping> testOutputTopic;

    @Inject
    KafkaStreamsProcessorMetrics metrics;

    @BeforeEach
    public void setup() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        testDriver = new TopologyTestDriver(topology, config);
        testInputTopic = testDriver.createInputTopic(kStreamsProcessorConfig.input().topic().get(),
                new StringSerializer(), new KafkaProtobufSerializer<>());
        testOutputTopic = testDriver.createOutputTopic(kStreamsProcessorConfig.output().topic().get(),
                new StringDeserializer(),
                new KafkaProtobufDeserializer<>(PingMessage.Ping.parser()));
    }

    @Test
    public void shouldCountProcessingErrorsInMetrics() {
        float errors = (float) metrics.processorErrorCounter().count();

        PingMessage.Ping input = newBuilder().setMessage("error").build();
        testInputTopic.pipeInput(input);

        // Check no produced messaged due to exception being thrown
        assertTrue(testOutputTopic.isEmpty());

        // Check amount of processed messages and errors reported in JSON response metrics is correct
        assertMetricEquals("\"kafkastreamsprocessor.processor.errors\"", errors + 1f);
    }

    @Test
    public void shouldCountPunctuationErrorsInMetrics() throws Exception {
        float punctuationErrors = (float) metrics.punctuatorErrorCounter().count();
        int thrownsBefore = TestProcessor.throwns;

        punctuatorThrowsException = true;
        testDriver.advanceWallClockTime(Duration.ofSeconds(1L));
        testDriver.advanceWallClockTime(Duration.ofSeconds(1L));
        testDriver.advanceWallClockTime(Duration.ofSeconds(1L));
        punctuatorThrowsException = false;

        // Check amount of processed messages and errors reported in JSON response metrics is correct
        assertMetricEquals("\"kafkastreamsprocessor.punctuation.errors\"",
                punctuationErrors + TestProcessor.throwns - thrownsBefore);
    }

    private <T> void assertMetricEquals(String jsonPath, float value) {
        given().accept(ContentType.JSON)
                .when()
                .get("/metrics")
                .then()
                .statusCode(200)
                .body(jsonPath, equalTo(value));
    }

    @Processor
    @Alternative
    public static class TestProcessor extends ContextualProcessor<String, PingMessage.Ping, String, PingMessage.Ping> {
        static int throwns;

        @Override
        public void init(ProcessorContext<String, PingMessage.Ping> context) {
            super.init(context);
            Punctuator exceptionPunctuator = timestamp -> {
                if (punctuatorThrowsException) {
                    throwns++;
                    throw new TestException();
                }
            };
            context.schedule(Duration.ofSeconds(1L), PunctuationType.WALL_CLOCK_TIME, exceptionPunctuator);
        }

        @Override
        public void process(Record<String, PingMessage.Ping> record) {
            if ("error".equals(record.value().getMessage())) {
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
    }
}
