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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.StringValue;

class IntrospectionSerializerTest {

    IntrospectionSerializer serializer;

    ObjectMapper mapper;

    @BeforeEach
    void setup() {
        mapper = mock(ObjectMapper.class);
        serializer = new IntrospectionSerializer(mapper);
    }

    @Test
    void shouldSerializeJSonWhenPayloadIsPojo() throws JsonProcessingException {
        JsonPojo pojo = new JsonPojo();
        serializer.serialize("whatever", pojo);
        verify(mapper).writeValueAsBytes(pojo);
    }

    @Test
    void shouldUseProtobufByteArraySerializationWhenPayloadIsProto() {
        StringValue proto = StringValue.of("test");
        assertThat(serializer.serialize("whatever", proto), is(StringValue.of("test").toByteArray()));
        verifyNoInteractions(mapper);
    }

    @Test
    void shouldGenerateIllegalArgumentExceptionWhenASerializationOccurs() throws JsonProcessingException {
        JsonProcessingException exception = mock(JsonProcessingException.class);
        when(mapper.writeValueAsBytes(any())).thenThrow(exception);
        assertThrows(IllegalArgumentException.class, () -> serializer.serialize("whatever", new JsonPojo()));
    }

    static class JsonPojo {
    }
}
