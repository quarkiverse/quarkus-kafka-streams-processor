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
package io.quarkiverse.kafkastreamsprocessor.kafka.streams.test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkiverse.kafkastreamsprocessor.api.exception.RetryableException;
import io.quarkus.builder.BuildChainBuilder;
import io.quarkus.deployment.builditem.GeneratedResourceBuildItem;
import io.quarkus.deployment.builditem.nativeimage.ReflectiveClassBuildItem;
import io.quarkus.test.QuarkusUnitTest;

public class KafkaStreamsProcessorProcessorTest {

    private static volatile List<ReflectiveClassBuildItem> registeredClasses;

    @RegisterExtension
    static QuarkusUnitTest runner = new QuarkusUnitTest()
            .setArchiveProducer(() -> ShrinkWrap.create(JavaArchive.class)
                    .addClass(io.quarkiverse.kafkastreamsprocessor.kafka.streams.test.MyProcessor.class))
            .overrideConfigKey("kafkastreamsprocessor.input.topics", "ping-events")
            .overrideConfigKey("kafkastreamsprocessor.output.topic", "pong-events")
            .overrideConfigKey("quarkus.kafka-streams.topics", "ping-events,pong-events")
            .addBuildChainCustomizer(buildCustomizer());

    private static Consumer<BuildChainBuilder> buildCustomizer() {
        return chainBuilder -> chainBuilder.addBuildStep(
                context -> {
                    registeredClasses = context.consumeMulti(ReflectiveClassBuildItem.class);
                    checkProperClassesAreRegistered();
                })
                .consumes(ReflectiveClassBuildItem.class)
                .produces(GeneratedResourceBuildItem.class)
                .build();
    }

    private static void checkProperClassesAreRegistered() {
        assertNotNull(registeredClasses);

        List<String> allRegisteredClasses = registeredClasses.stream()
                .flatMap(c -> c.getClassNames().stream())
                .collect(Collectors.toList());

        assertThat(allRegisteredClasses, hasItem(MyProcessor.class.getName()));
        // Default retryOn exception for Fault Tolerance
        assertThat(allRegisteredClasses, hasItem(RetryableException.class.getName()));
    }

    @Test
    void shouldRegisterTypesForReflection() {
        // if it gets there, it succeeded
        assertNull(registeredClasses);
    }

}
