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
package io.quarkiverse.kafkastreamsprocessor.sample.stateful.global;

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

    TestInputTopic<String, String> testGlobalTopic;

    TestInputTopic<String, String> testGlobalTopicCapital;

    KeyValueStore<Object, Object> store;

    KeyValueStore<Object, Object> storeCapital;

    @BeforeEach
    void setup() {
        Properties config = new Properties();
        // TestDriver using stores write on disk, for parallelization it's important the folder is unique per Maven module
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "kstream-stateful-sample");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        testDriver = new TopologyTestDriver(topology, config);
        store = testDriver.getKeyValueStore("store-data");
        storeCapital = testDriver.getKeyValueStore("store-data-capital");

        testInputTopic = testDriver.createInputTopic(kStreamsProcessorConfig.input().topic().get(),
                new StringSerializer(),
                new KafkaProtobufSerializer<>());
        testOutputTopic = testDriver.createOutputTopic(kStreamsProcessorConfig.output().topic().get(),
                new StringDeserializer(),
                new KafkaProtobufDeserializer<>(Ping.parser()));

        testGlobalTopic = testDriver.createInputTopic(
                kStreamsProcessorConfig.globalStores().get("store-data").topic(),
                new StringSerializer(),
                new StringSerializer());

        testGlobalTopicCapital = testDriver.createInputTopic(
                kStreamsProcessorConfig.globalStores().get("store-data-capital").topic(),
                new StringSerializer(),
                new StringSerializer());
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }

    @Test
    public void testKeyNotInStore() {
        sendAndTest("ID1", "value1", "Stored value for ID1 is null");
        assertThat(storeCapital.get("ID1"), equalTo(null));
    }

    @Test
    public void processKeyWithExistingValueInStore() {
        testGlobalTopic.pipeInput("ID1", "dont-capitalize-me");
        testGlobalTopicCapital.pipeInput("ID1", "capitalize-me");
        sendAndTest("ID1", "whatever", "Stored value for ID1 is dont-capitalize-me and capitalized value is CAPITALIZE-ME");

        assertThat(store.get("ID1"), equalTo("dont-capitalize-me"));
        assertThat(storeCapital.get("ID1"), equalTo("CAPITALIZE-ME"));
    }

    void sendAndTest(String key, String message, String expectedResponse) {
        testInputTopic.pipeInput(key, Ping.newBuilder().setMessage(message).build());
        TestRecord<String, Ping> record = testOutputTopic.readRecord();
        // startsWith because answer can contain insertion timestamp
        assertThat(record.value().getMessage(), equalTo(expectedResponse));
    }
}
