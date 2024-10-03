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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.daniel.shuy.kafka.protobuf.serde.KafkaProtobufSerde;

import io.quarkiverse.kafkastreamsprocessor.api.serdes.JacksonSerde;
import io.quarkiverse.kafkastreamsprocessor.impl.IntrospectionSerializer;
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
    void shouldSetIntrospectionSerializerByDefault() {
        when(configuration.getProcessorPayloadType()).thenReturn((Class) JSonPojo.class);
        when(configuration.getSinkValueSerializer()).thenReturn(null);
        customizer.apply(configuration);
        verify(configuration).setSinkValueSerializer(any(IntrospectionSerializer.class));
    }

    @Test
    void shouldSetCustomizedSinkSerializerAndValueSerde() {
        Serde customSerde = mock(Serde.class);
        Serializer customSerialiazer = mock(Serializer.class);
        when(configuration.getSinkValueSerializer()).thenReturn(customSerialiazer);
        when(configuration.getSourceValueSerde()).thenReturn(customSerde);
        customizer.apply(configuration);
        assertThat(configuration.getSinkValueSerializer(), is(equalTo(customSerialiazer)));
        assertThat(configuration.getSourceValueSerde(), is(equalTo(customSerde)));
    }

    @Test
    void shouldSetProtobufSerdeWhenProcessorIsProtobuf() {
        when(configuration.getProcessorPayloadType()).thenReturn((Class) PingMessage.Ping.class);
        customizer.apply(configuration);
        verify(configuration).setSourceValueSerde(isA(KafkaProtobufSerde.class));
    }

    @Test
    void shouldSetJacksonSerdeWhenProcessorIsAPojo() {
        when(configuration.getProcessorPayloadType()).thenReturn((Class) JSonPojo.class);
        customizer.apply(configuration);
        verify(configuration).setSourceValueSerde(isA(JacksonSerde.class));
    }

    class JSonPojo {
    }
}
