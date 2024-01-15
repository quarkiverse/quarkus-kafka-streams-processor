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
package io.quarkiverse.kafkastreamsprocessor.impl.decorator.processor;

import java.util.logging.LogRecord;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import lombok.AllArgsConstructor;

/**
 * hamcrest {@link Matcher} to ease logRecords assertion
 */
@AllArgsConstructor
public class IsLogRecord extends TypeSafeMatcher<LogRecord> {

    private LogRecord toCompare;

    @Override
    public void describeTo(Description description) {
        description.appendText("should match LogRecords " + toCompare.getLevel() + " " + toCompare.getMessage());
    }

    @Override
    protected boolean matchesSafely(LogRecord item) {
        boolean result = toCompare.getLevel().equals(item.getLevel()) && item.getMessage().contains(toCompare.getMessage());

        if (toCompare.getThrown() != null) {
            result = result && toCompare.getThrown().equals(item.getThrown());
        }

        return result;
    }

    /**
     * @param logRecord
     *        logRecord to match
     * @return if the logRecord is matching
     */
    public static Matcher<LogRecord> isLogRecord(LogRecord logRecord) {
        return new IsLogRecord(logRecord);
    }

}
