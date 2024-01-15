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
package io.quarkiverse.kafkastreamsprocessor.impl.decorator.processor;

import static io.quarkiverse.kafkastreamsprocessor.sample.message.PingMessage.Ping.newBuilder;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.Collections;
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

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.quarkiverse.kafkastreamsprocessor.api.Processor;
import io.quarkiverse.kafkastreamsprocessor.api.exception.RetryableException;
import io.quarkiverse.kafkastreamsprocessor.api.properties.KStreamsProcessorConfig;
import io.quarkiverse.kafkastreamsprocessor.sample.message.PingMessage;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import lombok.extern.slf4j.Slf4j;

/**
 * Test class of {@link RetryDecorator}
 */
@QuarkusTest
@TestProfile(RetryDecoratorQuarkusTest.SimpleProtobufProcessorProfile.class)
@Slf4j
class RetryDecoratorQuarkusTest {
    private final static Tag METHOD_TAG = Tag.of("method",
            "io.quarkiverse.kafkastreamsprocessor.impl.decorator.processor.RetryDecoratorDelegate.retryableProcess");
    private final static Tag RETRIED_TAG = Tag.of("retried", "true");
    private final static Tag VALUE_RETURNED_TAG = Tag.of("retryResult", "valueReturned");
    private final static Tag RESULT_VALUE_RETURNED_TAG = Tag.of("result", "valueReturned");

    static int processorCall = 0;

    @Inject
    KStreamsProcessorConfig kStreamsProcessorConfig;

    TopologyTestDriver testDriver;

    @Inject
    Topology topology;

    @Inject
    MeterRegistry registry;

    TestInputTopic<String, PingMessage.Ping> testInputTopic;

    TestOutputTopic<String, PingMessage.Ping> testOutputTopic;

    @BeforeEach
    public void setUp() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        testDriver = new TopologyTestDriver(topology, config);
        testInputTopic = testDriver.createInputTopic(kStreamsProcessorConfig.input().topic().get(), new StringSerializer(),
                new KafkaProtobufSerializer<PingMessage.Ping>());
        testOutputTopic = testDriver.createOutputTopic(kStreamsProcessorConfig.output().topic().get(),
                new StringDeserializer(),
                new KafkaProtobufDeserializer<>(PingMessage.Ping.parser()));
    }

    @Test
    void shouldCountMessagesProcessedInMetrics() {
        PingMessage.Ping input = newBuilder().setMessage("test message").build();
        testInputTopic.pipeInput(input);
        testInputTopic.pipeInput(input);
        testInputTopic.pipeInput(input);

        // Check corresponding output message
        assertEquals("test message", testOutputTopic.readRecord().value().getMessage());
        assertEquals(9, processorCall); // (2 retry + 1)*3

        // 1 succeeded retried * 3
        assertCounterEquals("ft.retry.calls.total", Arrays.asList(METHOD_TAG,
                RETRIED_TAG,
                VALUE_RETURNED_TAG), 3, registry);
        // 3 processed messages
        assertCounterEquals("ft.invocations.total", Arrays.asList(METHOD_TAG, RESULT_VALUE_RETURNED_TAG), 3, registry);
        // 2 retries in error * 3
        assertCounterEquals("ft.retry.retries.total",
                Arrays.asList(METHOD_TAG), 6, registry);
    }

    private void assertCounterEquals(String meterName, Iterable<Tag> tags, int value, MeterRegistry registry) {
        Counter counter = registry.get(meterName).tags(tags).counter();
        assertEquals(value, (int) counter.count());
    }

    @Processor
    @Alternative
    public static class TestProcessor extends ContextualProcessor<String, PingMessage.Ping, String, PingMessage.Ping> {

        @Override
        public void process(Record<String, PingMessage.Ping> record) {
            processorCall++;

            if (processorCall % 3 != 0) {
                throw new RetryableException();
            }

            context().forward(record);
        }
    }

    public static class SimpleProtobufProcessorProfile implements QuarkusTestProfile {
        @Override
        public Set<Class<?>> getEnabledAlternatives() {
            return Collections.singleton(TestProcessor.class);
        }
    }

}
