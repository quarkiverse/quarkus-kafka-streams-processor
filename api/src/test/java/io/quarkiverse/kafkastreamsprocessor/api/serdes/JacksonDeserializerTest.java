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
package io.quarkiverse.kafkastreamsprocessor.api.serdes;

import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

class JacksonDeserializerTest {

    JacksonDeserializer<Pojo> deserializer;

    ObjectMapper mapper;

    @BeforeEach
    public void setup() {
        mapper = new ObjectMapper();
        deserializer = new JacksonDeserializer<>(Pojo.class);
    }

    @Test
    void shouldDeserializeAPojo() throws JsonProcessingException {
        Pojo pojo = new Pojo("value");
        Pojo deserialized = deserializer.deserialize("topic", mapper.writeValueAsBytes(pojo));
        Assertions.assertEquals(pojo, deserialized);
    }

    @Test
    void shouldReturnNullWhenNoPojoIsProvided() {
        Assertions.assertNull(deserializer.deserialize("topic", null));
    }

    @Test
    void shouldRaiseSerializationExceptionWhenJsonBytesAreNotValid() {
        Assertions.assertThrows(SerializationException.class,
                () -> deserializer.deserialize("topic", "NoJSONBytes".getBytes()));
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Pojo {
        String field;
    }
}
