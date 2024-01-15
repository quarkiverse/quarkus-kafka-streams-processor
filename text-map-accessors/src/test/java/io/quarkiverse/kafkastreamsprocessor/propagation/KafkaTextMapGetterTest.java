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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

import java.nio.charset.StandardCharsets;

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

class KafkaTextMapGetterTest {
    KafkaTextMapGetter getter = new KafkaTextMapGetter();

    RecordHeaders headers = new RecordHeaders();

    @Test
    void noKeys() {
        assertThat(getter.keys(headers), emptyIterable());
    }

    @Test
    void keys() {
        headers.add("blabla", new byte[0]);
        headers.add("blibli", new byte[0]);

        assertThat(getter.keys(headers), containsInAnyOrder("blabla", "blibli"));
    }

    @Test
    void noValue() {
        assertThat(getter.get(headers, "blabla"), nullValue());
    }

    @Test
    void value() {
        headers.add("blabla", "blabla".getBytes(StandardCharsets.UTF_8));
        headers.add("blibli", "blibli".getBytes(StandardCharsets.UTF_8));
        headers.add("blibli", "bloblo".getBytes(StandardCharsets.UTF_8));

        assertThat(getter.get(headers, "blabla"), equalTo("blabla"));
        assertThat(getter.get(headers, "blibli"), equalTo("bloblo"));
    }

    @Test
    void carrierNull() {
        assertThat(getter.get(null, "blabla"), nullValue());
    }

    @Test
    void headerKeyButNullValue() {
        headers.add("blabla", null);

        assertThat(getter.get(headers, "blabla"), nullValue());
    }

}
