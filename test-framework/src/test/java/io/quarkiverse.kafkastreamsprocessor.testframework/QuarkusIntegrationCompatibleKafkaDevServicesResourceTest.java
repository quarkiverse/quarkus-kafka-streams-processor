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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.quarkus.test.common.DevServicesContext;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

@ExtendWith(MockitoExtension.class)
class QuarkusIntegrationCompatibleKafkaDevServicesResourceTest {
    QuarkusIntegrationCompatibleKafkaDevServicesResource resource = new QuarkusIntegrationCompatibleKafkaDevServicesResource();

    @Mock
    DevServicesContext context;

    Map<String, String> devServiceProps = new HashMap<>();

    TestClass testInstance = new TestClass();

    QuarkusTestResourceLifecycleManager.TestInjector testInjector = new DefaultTestInjectorCopy(testInstance);

  @BeforeEach
  void setUp() {
    when(context.devServicesProperties()).thenReturn(devServiceProps);
  }

    @Test
    public void noKafkaBootstrapServersForwardedSoNothingSet() {
        resource.setIntegrationTestContext(context);
        resource.inject(testInjector);

        assertThat(testInstance.kafkaBootstrapServers1, nullValue());
        assertThat(testInstance.kafkaBootstrapServers2, equalTo(0));
        assertThat(testInstance.kafkaBootstrapServers3, nullValue());
    }

    @Test
    public void kafkaBootstrapServersCaughtFromDevServiceAndForwarded() {
        devServiceProps.put("kafka.bootstrap.servers", "OUTSIDE://localhost:42563");

        resource.setIntegrationTestContext(context);
        resource.inject(testInjector);

        assertThat(testInstance.kafkaBootstrapServers1, nullValue());
        assertThat(testInstance.kafkaBootstrapServers2, equalTo(0));
        assertThat(testInstance.kafkaBootstrapServers3, equalTo("OUTSIDE://localhost:42563"));
    }

    public static class TestClass {
        String kafkaBootstrapServers1;
        @KafkaBootstrapServers
        int kafkaBootstrapServers2;
        @KafkaBootstrapServers
        String kafkaBootstrapServers3;
    }

    // copy of TestResourceManager.DefaultTestInjector
    static class DefaultTestInjectorCopy implements QuarkusTestResourceLifecycleManager.TestInjector {

        // visible for testing
        final Object testInstance;

        DefaultTestInjectorCopy(Object testInstance) {
            this.testInstance = testInstance;
        }

        @Override
        public void injectIntoFields(Object fieldValue, Predicate<Field> predicate) {
            Class<?> c = testInstance.getClass();
            while (c != Object.class) {
                for (Field f : c.getDeclaredFields()) {
                    if (predicate.test(f)) {
                        f.setAccessible(true);
                        try {
                            f.set(testInstance, fieldValue);
                            return;
                        } catch (Exception e) {
                            throw new RuntimeException("Unable to set field '" + f.getName()
                                    + "' using 'QuarkusTestResourceLifecycleManager.TestInjector' ", e);
                        }
                    }
                }
                c = c.getSuperclass();
            }
            // no need to warn here because it's perfectly valid to have tests that don't use the injected fields
        }

    }
}
