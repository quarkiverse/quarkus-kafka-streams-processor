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
package io.quarkiverse.kafkastreamsprocessor.sample.customserde;

import java.nio.charset.StandardCharsets;

import jakarta.annotation.Priority;
import jakarta.decorator.Decorator;
import jakarta.decorator.Delegate;
import jakarta.inject.Inject;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;

import io.quarkiverse.kafkastreamsprocessor.api.decorator.processor.ProcessorDecoratorPriorities;

@Decorator
@Priority(ProcessorDecoratorPriorities.PUNCTUATOR_DECORATION + 2)
public class HeaderDecorator<KIn, VIn, KOut, VOut> implements Processor<KIn, VIn, KOut, VOut> {
    @lombok.experimental.Delegate(excludes = Excludes.class)
    private final Processor<KIn, VIn, KOut, VOut> delegate;

    @Inject
    public HeaderDecorator(@Delegate Processor<KIn, VIn, KOut, VOut> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void process(Record<KIn, VIn> record) {
        Header header = record.headers().lastHeader("custom-header");
        if (header != null) {
            String value = new String(header.value(), StandardCharsets.UTF_8);
            if (value.contains("error")) {
                throw new IllegalStateException("Error in header");
            }
        }
        delegate.process(record);
    }

    private interface Excludes {
        <KIn, VIn> void process(Record<KIn, VIn> record);
    }

}
