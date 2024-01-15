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
package io.quarkiverse.kafkastreamsprocessor.testframework;

import java.util.Map;

import io.quarkus.test.common.DevServicesContext;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import lombok.extern.slf4j.Slf4j;

/**
 * Lifecycle Manager to plug with @QuarkusTestResource that allows access to the kafka dev service even in a
 * QuarkusIntegrationTest.
 */
@Slf4j
public class QuarkusIntegrationCompatibleKafkaDevServicesResource
        implements QuarkusTestResourceLifecycleManager, DevServicesContext.ContextAware {
    String kafkaBootstrapServers;

    @Override
    public void setIntegrationTestContext(DevServicesContext context) {
        kafkaBootstrapServers = context.devServicesProperties().get("kafka.bootstrap.servers");
        if (kafkaBootstrapServers == null) {
            log.warn("Did not receive any kafka.bootstrap.servers property value from the DevServiceContext!");
        } else {
            log.info("Received kafka.bootstrap.servers = {}", kafkaBootstrapServers);
        }
    }

    @Override
    public Map<String, String> start() {
        return Map.of();
    }

    @Override
    public void stop() {
        // do nothing
    }

    @Override
    public void inject(TestInjector testInjector) {
        testInjector.injectIntoFields(kafkaBootstrapServers, field -> field.getType() == String.class &&
                field.getAnnotationsByType(KafkaBootstrapServers.class).length > 0);
    }
}
