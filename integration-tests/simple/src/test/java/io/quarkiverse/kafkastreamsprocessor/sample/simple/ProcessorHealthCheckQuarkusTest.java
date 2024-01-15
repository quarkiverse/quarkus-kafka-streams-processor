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

import static io.restassured.RestAssured.given;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.equalTo;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import jakarta.inject.Inject;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.awaitility.Durations;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import io.quarkiverse.kafkastreamsprocessor.api.properties.KStreamsProcessorConfig;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.restassured.response.ValidatableResponse;
import lombok.extern.slf4j.Slf4j;

@QuarkusTest
@TestProfile(ProcessorHealthCheckQuarkusTest.ProcessorHealthCheckQuarkusTestProfile.class)
@Slf4j
public class ProcessorHealthCheckQuarkusTest {

    @Inject
    KStreamsProcessorConfig kStreamsProcessorConfig;

    @Inject
    KafkaStreams streams;

    @ConfigProperty(name = "kafka.bootstrap.servers")
    String kafkaBootstrapServers;

    @Test
    @Timeout(15)
    public void testHealthChecksDuringLifecycle() throws Exception {
        waitKafkaStabilizes();

        checkLiveness().responseCode(200).isUp(true).isStateUp(true);
        checkReadiness().responseCode(200)
                .isUp(true)
                .topicsCheck(true,
                        kStreamsProcessorConfig.input().topic().get() + ","
                                + kStreamsProcessorConfig.output().topic().get(),
                        null)
                .stateCheck(true, "RUNNING");

        try (AdminClient admin = AdminClient
                .create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers))) {
            admin.deleteTopics(Collections.singletonList(kStreamsProcessorConfig.output().topic().get())).all().get(
                    10L,
                    TimeUnit.SECONDS);
        }

        checkLiveness().responseCode(200).isUp(true).isStateUp(true);
        checkReadiness().responseCode(503)
                .isUp(false)
                .topicsCheck(false, kStreamsProcessorConfig.input().topic().get(),
                        kStreamsProcessorConfig.output().topic().get())
                .stateCheck(true, "RUNNING");

        streams.close();

        checkLiveness().responseCode(503).isUp(false).isStateUp(false);
    }

    private void waitKafkaStabilizes() throws InterruptedException {
        await().atMost(Durations.TEN_SECONDS).until(() -> {
            boolean result = streams.state() == KafkaStreams.State.RUNNING;
            if (!result) {
                log.info("Wait until KafkaStreams state is RUNNING. Current state {}", streams.state()
                        .name());
            }
            return streams.state() == KafkaStreams.State.RUNNING;
        });
    }

    private LivenessValidation checkLiveness() {
        return new LivenessValidation();
    }

    private static class LivenessValidation {
        private ValidatableResponse response;

        LivenessValidation() {
            response = given().when()
                    .get("/q/health/live")
                    .then()
                    .body("checks.size()", equalTo(1));
        }

        LivenessValidation responseCode(int responseCode) {
            response = response.statusCode(responseCode);
            return this;
        }

        LivenessValidation isUp(boolean isUp) {
            response = response.body("status", isUpMatcher(isUp));
            return this;
        }

        LivenessValidation isStateUp(boolean isUp) {
            response = response.body("checks.find {it.name == 'Kafka Streams state health check'}.status", isUpMatcher(isUp));
            return this;
        }
    }

    private ReadinessValidation checkReadiness() {
        return new ReadinessValidation();
    }

    private static class ReadinessValidation {
        private ValidatableResponse response;

        ReadinessValidation() {
            response = given().when()
                    .get("/q/health/ready")
                    .then()
                    .body("checks.size()", equalTo(2));
        }

        ReadinessValidation responseCode(int responseCode) {
            response = response.statusCode(responseCode);
            return this;
        }

        ReadinessValidation isUp(boolean isUp) {
            response = response.body("status", isUpMatcher(isUp));
            return this;
        }

        ReadinessValidation topicsCheck(boolean isUp, String availableTopics, String missingTopics) {
            response = response.rootPath("checks.find {it.name == 'Kafka Streams topics health check'}")
                    .body("status", isUpMatcher(isUp))
                    .body("data.available_topics", equalTo(availableTopics))
                    .body("data.missing_topics", equalTo(missingTopics))
                    .noRootPath();
            return this;
        }

        ReadinessValidation stateCheck(boolean isUp, String state) {
            response = response.rootPath("checks.find {it.name == 'Kafka Streams state health check'}")
                    .body("status", isUpMatcher(isUp))
                    .body("data.state", equalTo(state))
                    .noRootPath();
            return this;
        }
    }

    private static Matcher<String> isUpMatcher(boolean isUp) {
        return equalTo(isUp ? "UP" : "DOWN");
    }

    /**
     * Use separate profile to restart the broker and avoid errors "Offset commit failed on partition ping-events-0 at
     * offset 2: This is not the correct coordinator."
     */
    public static class ProcessorHealthCheckQuarkusTestProfile implements QuarkusTestProfile {

    }
}
