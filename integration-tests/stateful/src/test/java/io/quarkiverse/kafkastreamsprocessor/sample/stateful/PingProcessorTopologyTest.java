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
package io.quarkiverse.kafkastreamsprocessor.sample.stateful;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.Properties;

import jakarta.inject.Inject;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.github.daniel.shuy.kafka.protobuf.serde.KafkaProtobufDeserializer;
import com.github.daniel.shuy.kafka.protobuf.serde.KafkaProtobufSerializer;

import io.quarkiverse.kafkastreamsprocessor.sample.message.PingMessage.Ping;
import io.quarkiverse.kafkastreamsprocessor.spi.properties.KStreamsProcessorConfig;
import io.quarkiverse.kafkastreamsprocessor.testframework.StateDirCleaningResource;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;

@QuarkusTest
@QuarkusTestResource(StateDirCleaningResource.class)
class PingProcessorTopologyTest {

    @Inject
    KStreamsProcessorConfig kStreamsProcessorConfig;

    TopologyTestDriver testDriver;

    @Inject
    Topology topology;

    TestInputTopic<String, Ping> testInputTopic;

    TestOutputTopic<String, Ping> testOutputTopic;

    KeyValueStore<Object, Object> store;

    @BeforeEach
    void setup() {
        Properties config = new Properties();
        // TestDriver using stores write on disk, for parallelization it's important the folder is unique per Maven module
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "kstream-stateful-sample");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        testDriver = new TopologyTestDriver(topology, config);
        store = testDriver.getKeyValueStore("ping-data");

        testInputTopic = testDriver.createInputTopic(kStreamsProcessorConfig.input().topic().get(),
                new StringSerializer(),
                new KafkaProtobufSerializer<>());
        testOutputTopic = testDriver.createOutputTopic(kStreamsProcessorConfig.output().topic().get(),
                new StringDeserializer(),
                new KafkaProtobufDeserializer<>(Ping.parser()));
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }

    @Test
    public void testKeyNotInStore() {
        store.put("ID1", null);
        sendAndTest("ID1", "value1", "Store initialization OK for ID1");
        assertThat(store.get("ID1"), equalTo("value1"));
    }

    @Test
    public void testKeyWithPreviousValueInStore() {
        store.put("ID1", "value1");
        sendAndTest("ID1", "value2", "Previous value for ID1 is value1");
        assertThat(store.get("ID1"), equalTo("value2"));
    }

    void sendAndTest(String key, String message, String expectedResponse) {
        testInputTopic.pipeInput(key, Ping.newBuilder().setMessage(message).build());
        TestRecord<String, Ping> record = testOutputTopic.readRecord();
        // startsWith because answer can contain insertion timestamp
        assertThat(record.value().getMessage(), equalTo(expectedResponse));
    }
}
