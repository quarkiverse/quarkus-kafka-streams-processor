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
package io.quarkiverse.kafkastreamsprocessor.spi.properties;

import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;

import org.eclipse.microprofile.faulttolerance.Retry;

import io.smallrye.config.WithDefault;

public interface RetryConfig {

    /**
     * Max number of retries.
     *
     * @see Retry#maxRetries()
     */
    @WithDefault("-1")
    int maxRetries();

    /**
     * The delay between retries.
     *
     * @see Retry#delay()
     */
    @WithDefault("0")
    long delay();

    /**
     * The unit for {@link #delay()}. Default milliseconds.
     *
     * @see Retry#delayUnit()
     */
    @WithDefault("MILLIS")
    ChronoUnit delayUnit();

    /**
     * The max duration.
     *
     * @see Retry#maxDuration()
     */
    @WithDefault("180000")
    long maxDuration();

    /**
     * The duration unit for {@link #maxDuration()}.
     * <p>
     * Milliseconds by default.
     * </p>
     *
     * @see Retry#durationUnit()
     */
    @WithDefault("MILLIS")
    ChronoUnit durationUnit();

    /**
     * Jitter value to randomly vary retry delays for.
     *
     * @see Retry#jitter()
     */
    @WithDefault("200")
    long jitter();

    /**
     * The delay unit for {@link #jitter()}. Default is milliseconds.
     *
     * @see Retry#jitterDelayUnit()
     */
    @WithDefault("MILLIS")
    ChronoUnit jitterDelayUnit();

    /**
     * The list of exception types that should trigger a retry.
     * <p>
     * Default is the provided {@link io.quarkiverse.kafkastreamsprocessor.api.exception.RetryableException}.
     * </p>
     *
     * @see Retry#retryOn()
     */
    @WithDefault("io.quarkiverse.kafkastreamsprocessor.api.exception.RetryableException")
    List<Class<? extends Throwable>> retryOn();

    /**
     * The list of exception types that should <i>not</i> trigger a retry.
     * <p>
     * Default is empty list
     * </p>
     *
     * @see Retry#abortOn()
     */
    @WithDefault("")
    Optional<List<Class<? extends Throwable>>> abortOn();
}