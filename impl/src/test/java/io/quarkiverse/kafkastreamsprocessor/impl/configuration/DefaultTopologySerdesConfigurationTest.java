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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.daniel.shuy.kafka.protobuf.serde.KafkaProtobufSerde;

import io.quarkiverse.kafkastreamsprocessor.api.serdes.JacksonSerde;
import io.quarkiverse.kafkastreamsprocessor.impl.IntrospectionSerializer;
import io.quarkiverse.kafkastreamsprocessor.impl.StringIntrospectionSerializer;
import io.quarkiverse.kafkastreamsprocessor.sample.message.PingMessage;

class DefaultTopologySerdesConfigurationTest {

    DefaultTopologySerdesConfiguration customizer;

    ObjectMapper objectMapper;

    TopologyConfigurationImpl configuration;

    @BeforeEach
    void setup() {
        configuration = mock(TopologyConfigurationImpl.class);
        objectMapper = mock(ObjectMapper.class);
        customizer = new DefaultTopologySerdesConfiguration(objectMapper);
    }

    @Test
    void valueDefaultIntrospectionSerializer() {
        when(configuration.getProcessorKeyType()).thenReturn((Class) String.class);
        when(configuration.getProcessorPayloadType()).thenReturn((Class) JSonPojo.class);
        when(configuration.getSinkValueSerializer()).thenReturn(null);
        customizer.apply(configuration);
        verify(configuration).setSinkValueSerializer(any(IntrospectionSerializer.class));
    }

    @Test
    void leaveConfiguredValueSourceSerdeAndSinkSerializer() {
        when(configuration.getProcessorKeyType()).thenReturn((Class) String.class);
        Serde customSerde = mock(Serde.class);
        Serializer customSerialiazer = mock(Serializer.class);
        when(configuration.getSinkValueSerializer()).thenReturn(customSerialiazer);
        when(configuration.getSourceValueSerde()).thenReturn(customSerde);
        customizer.apply(configuration);
        verify(configuration, never()).setSinkValueSerializer(any());
        verify(configuration, never()).setSourceValueSerde(any());
    }

    @Test
    void valueProtobufSerde() {
        when(configuration.getProcessorKeyType()).thenReturn((Class) String.class);
        when(configuration.getProcessorPayloadType()).thenReturn((Class) PingMessage.Ping.class);
        customizer.apply(configuration);
        verify(configuration).setSourceValueSerde(isA(KafkaProtobufSerde.class));
    }

    @Test
    void valueJacksonSerde() {
        when(configuration.getProcessorKeyType()).thenReturn((Class) String.class);
        when(configuration.getProcessorPayloadType()).thenReturn((Class) JSonPojo.class);
        customizer.apply(configuration);
        verify(configuration).setSourceValueSerde(isA(JacksonSerde.class));
    }

    class JSonPojo {
    }

    @Test
    void keyStringIntrospectionSerializer() {
        when(configuration.getProcessorKeyType()).thenReturn((Class) JSonPojo.class);
        when(configuration.getProcessorPayloadType()).thenReturn((Class) String.class);
        when(configuration.getSinkKeySerializer()).thenReturn(null);
        customizer.apply(configuration);
        verify(configuration).setSinkKeySerializer(any(StringIntrospectionSerializer.class));
    }

    @Test
    void leaveCustomKeySourceSerdeAndSinkSerializer() {
        when(configuration.getProcessorPayloadType()).thenReturn((Class) String.class);
        Serde customSerde = mock(Serde.class);
        Serializer customSerialiazer = mock(Serializer.class);
        when(configuration.getSinkKeySerializer()).thenReturn(customSerialiazer);
        when(configuration.getSourceKeySerde()).thenReturn(customSerde);
        customizer.apply(configuration);
        verify(configuration, never()).setSinkKeySerializer(any());
        verify(configuration, never()).setSourceKeySerde(any());
    }

    @Test
    void keyProtobufSerde() {
        when(configuration.getProcessorKeyType()).thenReturn((Class) PingMessage.Ping.class);
        when(configuration.getProcessorPayloadType()).thenReturn((Class) String.class);
        customizer.apply(configuration);
        verify(configuration).setSourceKeySerde(isA(KafkaProtobufSerde.class));
    }

    @Test
    void keyStringSerde() {
        when(configuration.getProcessorKeyType()).thenReturn((Class) JSonPojo.class);
        when(configuration.getProcessorPayloadType()).thenReturn((Class) String.class);
        customizer.apply(configuration);
        verify(configuration).setSourceKeySerde(isA(Serdes.StringSerde.class));
    }
}
