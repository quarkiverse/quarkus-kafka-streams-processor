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
package io.quarkiverse.kafkastreamsprocessor.impl.errors;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.verify;

import java.util.Map;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class GlobalDLQProductionExceptionHandlerTest {

    @Mock
    private GlobalDLQProductionExceptionHandlerDelegate delegate;

    @Mock
    ProducerRecord<byte[], byte[]> record;

    private GlobalDLQProductionExceptionHandler handler;

    @BeforeEach
    public void setUp() {
        handler = new GlobalDLQProductionExceptionHandler(delegate);
    }

    @Test
    public void shouldDelegateToCdiHandler() {
        RuntimeException exception = new RuntimeException();
        handler.handle(record, exception);
        verify(delegate).handle(eq(record), same(exception));
    }

    @Test
    public void shouldConfigureTheDelegate() {
        Map<String, Object> configuration = Map.of();
        handler.configure(configuration);
        verify(delegate).configure(same(configuration));
    }
}
