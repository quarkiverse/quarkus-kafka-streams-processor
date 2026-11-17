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
package io.quarkiverse.kafkastreamsprocessor.sample.consumer;

import static io.restassured.RestAssured.given;
import static io.restassured.RestAssured.when;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.is;

import java.time.Duration;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.kafka.InjectKafkaCompanion;
import io.quarkus.test.kafka.KafkaCompanionResource;
import io.smallrye.reactive.messaging.kafka.companion.KafkaCompanion;
import io.smallrye.reactive.messaging.kafka.companion.ProducerBuilder;

/**
 * Blackbox test that can run both in JVM and native modes (@Inject and @ConfigProperty not allowed)
 */
@QuarkusTest
@QuarkusTestResource(KafkaCompanionResource.class)
public class ConsumerProcessorQuarkusTest {
    @InjectKafkaCompanion
    KafkaCompanion companion;

    String senderTopic = "input-topic";

    ProducerBuilder<String, String> producer;

    @BeforeEach
    public void setup() {
        producer = companion.produceWithSerializers(new StringSerializer(), new StringSerializer());
    }

    @AfterEach
    public void tearDown() {
        producer.close();
    }

    @Test
    public void shouldStoreConsumedEvents() throws Exception {
        // Ensure the storage is empty before the test
        given().when()
                .delete("/cached-events")
                .then()
                .statusCode(200);
        given().when()
                .get("/cached-events")
                .then()
                .statusCode(200)
                .body(is("No events cached"));

        producer.fromRecords(new ProducerRecord<>(senderTopic, "MyEventKey", "{ \"value\": \"Hello, World!\" }"))
                .awaitCompletion(Duration.ofSeconds(1));

        await().atMost(Duration.ofSeconds(10))
                .until(() -> "Key: MyEventKey, Value: MyData[value=Hello, World!]\n".equals(
                        when().get("/cached-events")
                                .then()
                                .statusCode(200)
                                .extract()
                                .body()
                                .asString()));
    }
}
