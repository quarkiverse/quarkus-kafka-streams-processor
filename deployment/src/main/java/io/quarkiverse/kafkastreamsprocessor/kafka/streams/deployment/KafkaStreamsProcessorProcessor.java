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
package io.quarkiverse.kafkastreamsprocessor.kafka.streams.deployment;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.jandex.AnnotationInstance;
import org.jboss.jandex.AnnotationTarget;
import org.jboss.jandex.DotName;

import io.quarkiverse.kafkastreamsprocessor.api.Processor;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.CombinedIndexBuildItem;
import io.quarkus.deployment.builditem.FeatureBuildItem;
import io.quarkus.deployment.builditem.nativeimage.ReflectiveClassBuildItem;

public class KafkaStreamsProcessorProcessor {

    private static final String FEATURE = "kafka-streams-processor";

    private static final DotName PROCESSOR_ANNOTATION = DotName.createSimple(Processor.class.getName());

    @BuildStep
    FeatureBuildItem feature() {
        return new FeatureBuildItem(FEATURE);
    }

    @BuildStep
    public void configureNativeExecutable(CombinedIndexBuildItem combinedIndex,
            BuildProducer<ReflectiveClassBuildItem> reflectiveClass) {
        for (AnnotationInstance processor : combinedIndex.getIndex().getAnnotations(PROCESSOR_ANNOTATION)) {
            AnnotationTarget target = processor.target();
            if (target.kind() == AnnotationTarget.Kind.CLASS) {
                reflectiveClass.produce(ReflectiveClassBuildItem.builder(target.asClass().name().toString())
                        .constructors()
                        .methods()
                        .fields()
                        .build());
            }
        }
    }

    @BuildStep
    public void registerRetryExceptions(BuildProducer<ReflectiveClassBuildItem> reflectiveClass) {
        Config config = ConfigProvider.getConfig();

        config.getOptionalValue("kafkastreamsprocessor.retry.retry-on", String[].class)
                .ifPresent(retryExceptions -> reflectiveClass.produce(ReflectiveClassBuildItem.builder(retryExceptions)
                        .methods(false)
                        .fields(false)
                        .build()));
        config.getOptionalValue("kafkastreamsprocessor.retry.abort-on", String[].class)
                .ifPresent(abortExceptions -> reflectiveClass.produce(ReflectiveClassBuildItem.builder(abortExceptions)
                        .methods(false)
                        .fields(false)
                        .build()));
    }
}
