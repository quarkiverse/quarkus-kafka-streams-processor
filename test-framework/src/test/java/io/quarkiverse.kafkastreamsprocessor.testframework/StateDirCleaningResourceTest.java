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
package io.quarkiverse.kafkastreamsprocessor.testframework;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

class StateDirCleaningResourceTest {

    @Test
    void createsTempDirAndDeletesIt() throws Exception {
        // If you are running this test with your IDE, make sure to add the
        // `--add-opens java.base/java.io=ALL-UNNAMED` JVM by editing the run configuration.

        StateDirCleaningResource resource = new StateDirCleaningResource();
        Map<String, String> props = resource.start();

        assertThat(props, hasEntry(equalTo("kafka-streams.state.dir"), containsString("kstreamstateful")));

        resource.stop();

        // Use reflection to access DeleteOnExitHook's private files field
        Class<?> deleteOnExitHookClass = Class.forName("java.io.DeleteOnExitHook");
        Field filesField = deleteOnExitHookClass.getDeclaredField("files");
        filesField.setAccessible(true);

        @SuppressWarnings("unchecked")
        Set<String> filesToDelete = (Set<String>) filesField.get(null);
        assertTrue(filesToDelete.stream().anyMatch(path -> path.contains("kstreamstateful")));
    }

}
