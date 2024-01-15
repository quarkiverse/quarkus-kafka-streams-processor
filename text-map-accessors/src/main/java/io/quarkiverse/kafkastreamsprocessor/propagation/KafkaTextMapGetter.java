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
package io.quarkiverse.kafkastreamsprocessor.propagation;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import io.opentelemetry.context.propagation.TextMapGetter;

@ApplicationScoped
public class KafkaTextMapGetter implements TextMapGetter<Headers> {
    @Override
    public Iterable<String> keys(Headers headers) {
        List<String> keys = new ArrayList<>();
        for (Header header : headers) {
            keys.add(header.key());
        }
        return keys;
    }

    @Nullable
    @Override
    public String get(@Nullable Headers headers, String key) {
        String value = null;
        if (headers != null) {
            Header lastHeader = headers.lastHeader(key);
            if (lastHeader != null && lastHeader.value() != null) {
                value = new String(lastHeader.value(), StandardCharsets.UTF_8);
            }
        }
        return value;
    }
}
