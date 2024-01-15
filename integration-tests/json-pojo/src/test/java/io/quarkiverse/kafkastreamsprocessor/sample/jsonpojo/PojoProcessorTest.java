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
package io.quarkiverse.kafkastreamsprocessor.sample.jsonpojo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.verify;

import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class PojoProcessorTest {

    PojoProcessor processor = new PojoProcessor();

    @Mock
    ProcessorContext<String, SamplePojo> context;

    @Captor
    ArgumentCaptor<Record<String, SamplePojo>> captor;

    @BeforeEach
    public void setup() {
        processor.init(context);
    }

    @Test
    public void replies_with_inverted_string() {
        SamplePojo pojo = new SamplePojo("hello", 1234, true);

        processor.process(new Record<>("key", pojo, 0L));

        verify(context).forward(captor.capture());
        SamplePojo pojo_response = captor.getValue().value();

        assertEquals("olleh", pojo_response.getStringField());
        assertEquals(1271, pojo_response.getNumericalField());
        assertFalse(pojo_response.getBooleanField());
    }
}
