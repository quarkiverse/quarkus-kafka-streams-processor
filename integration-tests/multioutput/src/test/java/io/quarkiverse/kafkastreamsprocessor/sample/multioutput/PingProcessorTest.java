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
package io.quarkiverse.kafkastreamsprocessor.sample.multioutput;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.quarkiverse.kafkastreamsprocessor.sample.message.PingMessage.Ping;

@ExtendWith(MockitoExtension.class)
public class PingProcessorTest {

    PingProcessor processor;

    @Mock
    ProcessorContext<String, Ping> context;

    @BeforeEach
    public void setup() {
        processor = new PingProcessor();
        processor.init(context);
    }

    @Test
    public void replies_with_pong() {
        Ping ping = Ping.newBuilder().setMessage("world").build();

        processor.process(new Record<>("key", ping, 0L));

        verify(context).forward(eq(new Record<>("key", Ping.newBuilder().setMessage("5").build(), 0L)), eq("pong"));
    }

    @Test
    public void replies_with_pang() {
        Ping ping = Ping.newBuilder().setMessage("error").build();

        processor.process(new Record<>("key", ping, 0L));

        verify(context).forward(eq(new Record<>("key", Ping.newBuilder().setMessage("PANG!").build(), 0L)), eq("pang"));
    }

}
