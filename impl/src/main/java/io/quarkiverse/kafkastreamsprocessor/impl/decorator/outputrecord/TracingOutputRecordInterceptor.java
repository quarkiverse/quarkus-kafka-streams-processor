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
package io.quarkiverse.kafkastreamsprocessor.impl.decorator.outputrecord;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.apache.kafka.streams.processor.api.Record;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.context.Context;
import io.quarkiverse.kafkastreamsprocessor.api.decorator.outputrecord.OutputRecordInterceptor;
import io.quarkiverse.kafkastreamsprocessor.api.decorator.outputrecord.OutputRecordInterceptorPriorities;
import io.quarkiverse.kafkastreamsprocessor.propagation.KafkaTextMapSetter;

/**
 * Producer interceptor that injects the tracing headers for propagation.
 */
@ApplicationScoped
public class TracingOutputRecordInterceptor implements OutputRecordInterceptor {
    private final OpenTelemetry openTelemetry;

    private final KafkaTextMapSetter kafkaTextMapSetter;

    @Inject
    public TracingOutputRecordInterceptor(OpenTelemetry openTelemetry, KafkaTextMapSetter kafkaTextMapSetter) {
        this.openTelemetry = openTelemetry;
        this.kafkaTextMapSetter = kafkaTextMapSetter;
    }

    @Override
    public Record interceptOutputRecord(Record record) {
        openTelemetry.getPropagators().getTextMapPropagator().fields().forEach(record.headers()::remove);
        openTelemetry.getPropagators()
                .getTextMapPropagator()
                .inject(Context.current(), record.headers(), kafkaTextMapSetter);
        return record;
    }

    @Override
    public int priority() {
        return OutputRecordInterceptorPriorities.TRACING;
    }
}
