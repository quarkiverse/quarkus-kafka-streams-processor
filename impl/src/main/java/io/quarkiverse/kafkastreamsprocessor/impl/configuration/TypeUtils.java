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
package io.quarkiverse.kafkastreamsprocessor.impl.configuration;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import jakarta.enterprise.inject.spi.BeanManager;
import jakarta.enterprise.util.TypeLiteral;

import org.apache.kafka.streams.processor.api.Processor;

import com.fasterxml.jackson.databind.type.TypeFactory;
import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;

import lombok.RequiredArgsConstructor;

/**
 * Helper class for type inference from kstream processor generic types
 */
public final class TypeUtils {

    private TypeUtils() {
    }

    static Parser<MessageLite> createParserFromType(Class<?> protobufMessageType) {
        try {
            if (GeneratedMessageV3.class.isAssignableFrom(protobufMessageType) ||
                    GeneratedMessage.class.isAssignableFrom(protobufMessageType)) {
                return (Parser<MessageLite>) protobufMessageType.getMethod("parser").invoke(null);
            } else {
                throw new IllegalArgumentException("Payload type " + protobufMessageType + " can not assigned to "
                        + GeneratedMessageV3.class + " or " + GeneratedMessage.class);
            }
        } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new IllegalArgumentException("Cannot instantiate a parser from type " + protobufMessageType, e);
        }
    }

    /**
     * Find the {@link Processor} provided (via the bean manager) and return its class
     */
    public static Class<?> reifiedProcessorType(BeanManager beanManager) {
        TypeLiteral<Processor<?, ?, ?, ?>> kafka3ProcessorType = new TypeLiteral<>() {
        };
        return beanManager.getBeans(kafka3ProcessorType.getType())
                .stream()
                .filter(b -> b.getStereotypes().contains(io.quarkiverse.kafkastreamsprocessor.api.Processor.class))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        "No bean found of type " + kafka3ProcessorType.getType()))
                .getBeanClass();
    }

    /**
     * Determine the type's payload type (V or Vin generic type arg of processor API). With Kafka 3.x, we can no longer
     * look at the signature of the process method as Record is also a generic type. However, we can look at the type
     * hierarchy of the processor.
     * <p>
     * Recursion stops at the first superclass or superinterface which is a parametrized type, and we assume the payload
     * type is the 2nd type argument, which works with Kafka 3 APIs. The Kafka 2 API is no longer supported.
     *
     * @return The payload type class, or null if the type hierarchy doesn't contain a parametrized type with at least 2
     *         arguments.
     */
    public static Class<?> extractPayloadType(Type type) {
        return new TypeExtractor(1).extractType(type);
    }

    /**
     * Determine the type's key type (K or Kin generic type arg of processor API). With Kafka 3.x, we can no longer look
     * at the signature of the process method as Record is also a generic type. However, we can look at the type hierarchy
     * of the processor.
     * <p>
     * Recursion stops at the first superclass or superinterface which is a parametrized type, and we assume the payload
     * type is the 1st type argument, which works for the Kafka 3 API. The Kafka 2 API is no longer supported.
     *
     * @return The key type class, or null if the type hierarchy doesn't contain a parametrized type with at least 2
     *         arguments.
     */
    public static Class<?> extractKeyType(Type type) {
        return new TypeExtractor(0).extractType(type);
    }

    @RequiredArgsConstructor
    private static class TypeExtractor {
        private final int genericIndex;

        public Class<?> extractType(Type type) {
            // If current class is parametrized, stop here.
            if (type instanceof ParameterizedType && ((ParameterizedType) type).getActualTypeArguments().length >= 2) {
                return TypeFactory.rawClass(((ParameterizedType) type).getActualTypeArguments()[genericIndex]);
            }

            // Recursion on superclass
            Class<?> clazz = TypeFactory.rawClass(type);
            if (clazz.getGenericSuperclass() != null) {
                Class<?> aType = extractType(clazz.getGenericSuperclass());
                if (aType != null) {
                    return aType;
                }
            }

            // Recursion on superinterfaces
            for (Type interfaceType : clazz.getGenericInterfaces()) {
                Class<?> aType = extractType(interfaceType);
                if (aType != null) {
                    return aType;
                }
            }
            return null;
        }
    }
}
