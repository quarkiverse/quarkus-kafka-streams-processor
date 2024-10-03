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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.when;

import org.apache.kafka.streams.KafkaStreams;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class RebalancingTopicHealthCheckTest {
    @Mock
    KafkaStreams streams;

    RebalancingTopicHealthCheck healthCheck = new RebalancingTopicHealthCheck();

    @BeforeEach
    public void setUp() {
        healthCheck.kafkaStreams = streams;
    }

    @Test
    public void testStateRunningHealthOK() {
        when(streams.state()).thenReturn(KafkaStreams.State.RUNNING);
        HealthCheckResponse response = healthCheck.call();
        assertThat(response.getStatus(), equalTo(HealthCheckResponse.Status.UP));
        assertThat(response.getData().get().get("state"), equalTo("RUNNING"));
    }

    @Test
    public void testUndefinedStateHealthKO() {
        HealthCheckResponse response = healthCheck.call();
        assertThat(response.getStatus(), equalTo(HealthCheckResponse.Status.DOWN));
        assertThat(response.getData().get().get("state"), nullValue());
    }

    @Test
    public void testStateRebalancingHealthKO() {
        when(streams.state()).thenReturn(KafkaStreams.State.REBALANCING);
        HealthCheckResponse response = healthCheck.call();
        assertThat(response.getStatus(), equalTo(HealthCheckResponse.Status.DOWN));
        assertThat(response.getData().get().get("state"), equalTo("REBALANCING"));
    }

    @Test
    public void testStateKOIfKakfaStreamsNotInjected() {
        // testing the NullPointerException cause it is the only possible Exception this code could return
        // indeed KafkaStreams#state() is a pure accessor
        healthCheck.kafkaStreams = null;
        HealthCheckResponse response = healthCheck.call();
        assertThat(response.getStatus(), equalTo(HealthCheckResponse.Status.DOWN));
        assertThat(response.getData().get(), hasKey("technical_error"));
    }

}
