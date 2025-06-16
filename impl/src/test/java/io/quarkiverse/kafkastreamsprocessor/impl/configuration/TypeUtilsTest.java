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

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TypeUtilsTest {

    @Mock
    TopologyConfigurationImpl config;

    @Test
    public void payloadTypeShouldBeExtractedFromKafka3Processor() {
        assertSame(Payload.class, TypeUtils.extractPayloadType(TestKafka3Processor.class));
    }

    @Test
    public void payloadTypeShouldBeExtractedFromSuperClassRecursively() {
        assertSame(Payload.class,
                TypeUtils.extractPayloadType(TestKafka3ProcessorWithClassRecursion.class));
    }

    @Test
    public void payloadTypeShouldBeExtractedFromInterfaceRecursively() {
        assertSame(Payload.class,
                TypeUtils.extractPayloadType(TestKafka3ProcessorWithInterfaceRecursion.class));
    }

    @Test
    public void payloadTypeShouldBeExtractedFromGenericSuperclass() {
        assertSame(Payload.class,
                TypeUtils.extractPayloadType(TestKafka3ProcessorWithGenerics.class));
    }

    @Test
    public void cannotDeterminePayloadType() {
        assertNull(TypeUtils.extractPayloadType(String.class));
    }

    @Test
    public void wrongProcessorPrototypeShouldNotBeAssignable() {
        Exception exception = assertThrows(IllegalArgumentException.class,
                () -> TypeUtils.createParserFromType(String.class));
        assertTrue(exception.getMessage().contains("can not assigned to"));
    }

    @Test
    public void payloadTypeWithInnerClass() {
        assertSame(Payload.Inner.class,
                TypeUtils.extractPayloadType(TestKafka3ProcessorWithInnerClass.class));
    }

    static class TestKafka3Processor extends ContextualProcessor<String, Payload, String, Payload> {

        @Override
        public void process(Record<String, Payload> record) {
        }
    }

    static class TestKafka3ProcessorWithInnerClass
            extends ContextualProcessor<String, Payload.Inner, String, Payload.Inner> {

        @Override
        public void process(Record<String, Payload.Inner> record) {
        }
    }

    static class Payload {

        static class Inner {

        }
    }

    static interface TestMyProcessor extends Processor<String, Payload, String, Payload> {

    }

    static class TestKafka3ProcessorWithInterfaceRecursion implements TestMyProcessor {

        @Override
        public void process(Record<String, Payload> record) {

        }
    }

    static abstract class TestMyAbstractProcessor extends ContextualProcessor<String, Payload, String, Payload> {

    }

    static class TestKafka3ProcessorWithClassRecursion extends TestMyAbstractProcessor {

        @Override
        public void process(Record<String, Payload> record) {

        }
    }

    static abstract class TestMyAbstractGenericProcessor<K, V> extends ContextualProcessor<K, V, K, V> {

    }

    static class TestKafka3ProcessorWithGenerics extends TestMyAbstractGenericProcessor<String, Payload> {

        @Override
        public void process(Record<String, Payload> record) {

        }
    }
}
