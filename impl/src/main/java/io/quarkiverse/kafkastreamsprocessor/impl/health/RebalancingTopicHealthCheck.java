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
package io.quarkiverse.kafkastreamsprocessor.impl.health;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.apache.kafka.streams.KafkaStreams;
import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.HealthCheckResponseBuilder;
import org.eclipse.microprofile.health.Readiness;

/**
 * Readiness probe that will fail whenever the state of the input topic is not RUNNING.
 * <p>
 * Indeed typically when the state is REBALANCING, while Kafka typically is reassigning partitions to consumers while a
 * change of setup is happening, the consumer is not ready to receive any messages. Adding this health check will
 * prevent automatic load tools like helm to wait longer until the rebalancing is done before integration tests are
 * started.
 * </p>
 */
@Readiness
@ApplicationScoped
public class RebalancingTopicHealthCheck implements HealthCheck {
    @Inject
    KafkaStreams kafkaStreams;

    /**
     * Executes the actual health check.
     *
     * @return a response object with additional informative fields:
     *         <ul>
     *         <li><code>state</code>: the actual state of the input topic when the check was run</li>
     *         <li><code>technical_error</code>: the exception message if any exception was raised while trying to fetch
     *         the topic state</li>
     *         </ul>
     */
    @Override
    public HealthCheckResponse call() {
        HealthCheckResponseBuilder responseBuilder = HealthCheckResponse.named("Kafka Streams state health check");
        try {
            KafkaStreams.State state = kafkaStreams.state();
            responseBuilder.status(state == KafkaStreams.State.RUNNING).withData("state", state.name());
        } catch (Exception e) {
            responseBuilder.down().withData("technical_error", e.getMessage());
        }
        return responseBuilder.build();
    }

}
