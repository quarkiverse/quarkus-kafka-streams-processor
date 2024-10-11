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
package io.quarkiverse.kafkastreamsprocessor.impl;

import java.lang.annotation.Annotation;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.context.RequestScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.spi.Bean;
import jakarta.enterprise.inject.spi.BeanManager;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;

import lombok.extern.slf4j.Slf4j;

/**
 * {@link ProcessorSupplier} used by the {@link TopologyProducer} to produce a {@link Processor} surrounded with some
 * decorators.
 */
@Dependent
@Slf4j
public class KStreamProcessorSupplier<KIn, VIn, KOut, VOut> implements ProcessorSupplier<KIn, VIn, KOut, VOut> {
    /**
     * Accessors to any Kafka 3's {@link Processor} implementations
     */
    private final Instance<Processor<?, ?, ?, ?>> kafka3BeanInstances;

    /**
     * Accessors to any Kafka 2's {@link org.apache.kafka.streams.processor.Processor} implementations
     */
    private final Instance<org.apache.kafka.streams.processor.Processor<?, ?>> beanInstances;

    /**
     * Builders of {@link Kafka2ProcessorAdapter} instances
     */
    private final Instance<Kafka2ProcessorAdapter<?, ?>> adapterInstances;

    /**
     * Injection constructor.
     *
     * @param kafka3BeanInstances
     *        accessors to any Kafka 3's {@link Processor} implementations
     * @param beanInstances
     *        accessors to any Kafka 2's {@link org.apache.kafka.streams.processor.Processor} implementations
     * @param adapterInstances
     *        builders of {@link Kafka2ProcessorAdapter} instances
     * @param beanManager
     *        the {@link BeanManager} instance to log the ordered list of {@link Processor} decorators declared in the
     *        framework and any extensions that might have been added
     */
    @Inject
    public KStreamProcessorSupplier(Instance<Processor<?, ?, ?, ?>> kafka3BeanInstances,
            Instance<org.apache.kafka.streams.processor.Processor<?, ?>> beanInstances,
            Instance<Kafka2ProcessorAdapter<?, ?>> adapterInstances, BeanManager beanManager) {
        this.kafka3BeanInstances = kafka3BeanInstances;
        this.beanInstances = beanInstances;
        this.adapterInstances = adapterInstances;

        log.info("Configured Processor decorators are in order: {}",
                beanManager.resolveDecorators(Set.of(Processor.class))
                        .stream()
                        .map(Bean::getBeanClass)
                        .map(Class::getName)
                        .collect(Collectors.joining(", ")));
    }

    /**
     * Returns one instance of the {@link Processor} (or {@link org.apache.kafka.streams.processor.Processor }) annotated
     * with {@link io.quarkiverse.kafkastreamsprocessor.api.Processor} annotation.
     * <p>
     * The instance is also decorated with the decorators logged by the constructor.
     * </p>
     *
     * @return a processor instance annotated with {@link io.quarkiverse.kafkastreamsprocessor.api.Processor}
     */
    @Override
    public Processor<KIn, VIn, KOut, VOut> get() {
        Processor<?, ?, ?, ?> processor;

        Optional<Processor<?, ?, ?, ?>> kafka3Processor = kafka3BeanInstances.stream()
                .filter(bean -> KStreamProcessorSupplier.hasAnnotation(bean,
                        io.quarkiverse.kafkastreamsprocessor.api.Processor.class))
                .findFirst();

        if (kafka3Processor.isEmpty()) {
            // Fallback to deprecated API, for backward compatibility.
            Optional<org.apache.kafka.streams.processor.Processor<?, ?>> kafka2Processor = beanInstances.stream()
                    .filter(bean -> KStreamProcessorSupplier.hasAnnotation(bean,
                            io.quarkiverse.kafkastreamsprocessor.api.Processor.class))
                    .findFirst();
            if (kafka2Processor.isEmpty()) {
                throw new IllegalArgumentException(
                        "No bean found of type " + io.quarkiverse.kafkastreamsprocessor.api.Processor.class);
            }
            Kafka2ProcessorAdapter<?, ?> processorAdapter = adapterInstances.get();
            processorAdapter.adapt((org.apache.kafka.streams.processor.Processor) kafka2Processor.get());
            processor = processorAdapter;
        } else {
            processor = kafka3Processor.get();
        }

        if (KStreamProcessorSupplier.hasAnnotation(processor, ApplicationScoped.class)
                || KStreamProcessorSupplier.hasAnnotation(processor, Singleton.class)
                || KStreamProcessorSupplier.hasAnnotation(processor, RequestScoped.class)) {
            throw new IllegalArgumentException(
                    "Processors cannot have a scope other than @Dependant, since KafkaStreams implementation classes are not thread-safe");
        }

        return (Processor<KIn, VIn, KOut, VOut>) processor;
    }

    private static boolean hasAnnotation(Object bean, Class<? extends Annotation> annotation) {
        // Microprofile annotations add a subClasses level, that's why we have to check at the parent level.
        // The current test with several microprofile annotations shows that it's not necessary do check upper than the
        // first superclass, but an iteration has been introduced to support maybe some unknown case.
        Class<?> current = bean.getClass();
        while (current != null) {
            if (current.isAnnotationPresent(annotation)) {
                return true;
            }
            current = current.getSuperclass();
        }
        return false;
    }

}
