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

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.io.FileUtils;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

/**
 * Test resource that forces to use a clean state dir for every start, goal being to clean potential state store status
 * from previous test runs.
 */
public class StateDirCleaningResource implements QuarkusTestResourceLifecycleManager {
    File tmpDir;

    @Override
    public Map<String, String> start() {
        try {
            tmpDir = File.createTempFile("kstreamstateful", "").getAbsoluteFile();
            if (!tmpDir.delete() || !tmpDir.mkdirs()) {
                throw new RuntimeException("Could not create dir " + tmpDir);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return Map.of("kafka-streams.state.dir", tmpDir.getAbsolutePath());
    }

    @Override
    public void stop() {
        try {
            FileUtils.forceDeleteOnExit(tmpDir);
        } catch (Exception e) {
            throw new RuntimeException("error deleting " + tmpDir, e);
        }
    }
}
