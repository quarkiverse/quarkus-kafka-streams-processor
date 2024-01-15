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
package io.quarkiverse.kafkastreamsprocessor.sample.stateful;

import static org.hamcrest.core.IsAnything.anything;
import static org.hamcrest.core.IsEqual.equalTo;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
class IsStateStoreContaining<K, V> extends TypeSafeMatcher<KeyValueStore<? extends K, ? extends V>> {
    private final Matcher<? extends K> keyMatcher;
    private final Matcher<? extends V> valueMatcher;

    @Override
    public boolean matchesSafely(KeyValueStore<? extends K, ? extends V> store) {
        KeyValueIterator<? extends K, ? extends V> it = store.all();
        while (it.hasNext()) {
            KeyValue<? extends K, ? extends V> next = it.next();
            if (keyMatcher.matches(next.key) && valueMatcher.matches(next.value)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void describeMismatchSafely(KeyValueStore<? extends K, ? extends V> store, Description mismatchDescription) {
        Map<K, V> map = new HashMap<>();
        KeyValueIterator<? extends K, ? extends V> it = store.all();
        while (it.hasNext()) {
            KeyValue<? extends K, ? extends V> next = it.next();
            map.put(next.key, next.value);
        }
        mismatchDescription.appendText("store was ").appendValueList("[", ", ", "]", map.entrySet());
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("store containing [")
                .appendDescriptionOf(keyMatcher)
                .appendText("->")
                .appendDescriptionOf(valueMatcher)
                .appendText("]");
    }

    /**
     * Creates a matcher for {@link KeyValueStore}s matching when the examined {@link KeyValueStore} contains at least one
     * key that is equal to the specified key. For example:
     *
     * <pre>
     * assertThat(myStore, hasKey("bar"))
     * </pre>
     *
     * @param key
     *        the key that satisfying key value stores must contain
     */
    public static <K> Matcher<KeyValueStore<? extends K, ?>> hasKey(K key) {
        return new IsStateStoreContaining<>(equalTo(key), anything());
    }
}
