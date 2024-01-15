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
package io.quarkiverse.kafkastreamsprocessor.sample.simple;

import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import jakarta.inject.Inject;

import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;

import de.svenjacobs.loremipsum.LoremIpsum;
import io.quarkiverse.kafkastreamsprocessor.api.Processor;
import io.quarkiverse.kafkastreamsprocessor.api.exception.RetryableException;
import io.quarkiverse.kafkastreamsprocessor.sample.message.PingMessage.Ping;
import lombok.extern.slf4j.Slf4j;

/**
 * Given different input messages reacts differently to generate different use cases:
 * <ul>
 * <li>default: count nb chars of string received and return that as String</li>
 * <li>error string: generates an exception throwing</li>
 * <li>starts with bigmessage: generates a message of size indicated with</li>
 * </ul>
 */
@Slf4j
@Processor // <1>
public class PingProcessor extends ContextualProcessor<String, Ping, String, Ping> { // <2>
    private static final Pattern P_BIG_MESSAGE = Pattern.compile(
            "^[bB][iI][gG] ?[mM][Ee][Ss][Ss][Aa][Gg][Ee](.*[^\\d])?(?<nbBytes>\\d+)$");

    private static final Pattern P_RETRYABLE = Pattern.compile("^[rR][eE][tT][rR][yY](.*[^\\d])?(?<nbRetries>\\d+)$");

    RetryableExceptionTracker retryableExceptionTracker;

    @Inject
    public PingProcessor(RetryableExceptionTracker retryableExceptionTracker) {
        this.retryableExceptionTracker = retryableExceptionTracker;
        log.info("instantiated " + this.getClass().getName());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void process(Record<String, Ping> ping) { // <3>
        log.info("Process the message: {}", ping.value().getMessage());

        if ("error".equals(ping.value().getMessage())) {
            throw new IllegalArgumentException("We have thrown an error!");
        }

        String resultMessage;
        Matcher bigMessageMatcher = P_BIG_MESSAGE.matcher(ping.value().getMessage());
        Matcher retryableMatcher = P_RETRYABLE.matcher(ping.value().getMessage());
        if (bigMessageMatcher.matches()) {
            // Produce a big message
            resultMessage = bigMessage(bigMessageMatcher);
        } else if (retryableMatcher.matches()) {
            resultMessage = throwRetryableException(retryableMatcher);
        } else {
            // Default scenario: count the number of characters in input message
            resultMessage = countChars(ping.value().getMessage());
        }
        Ping pong = Ping.newBuilder().setMessage(resultMessage).build();

        context().forward(ping.withValue(pong)); // <4>
    }

    /**
     * Counts the nb of characters and returns it as string.
     */
    private String countChars(String input) {
        log.info("Count characters for: " + input);
        int characterCount = input.length();
        return String.valueOf(characterCount);
    }

    /**
     * Input is a string prefix with bigmessage and suffixed with an integer size.
     * <p>
     * Example: <code>bigmessage-1200000</code> will generate a message if size 1.2 megabytes.
     * </p>
     */
    private String bigMessage(Matcher bigMessageMatcher) {
        // Get the requested length to generate
        int nbBytes = Integer.parseInt(bigMessageMatcher.group("nbBytes"));
        log.info("Parsed length: {} bytes", nbBytes);

        // source: lorem ipsum, destination: big array
        String bigMessage = generateBigLoremIpsum(nbBytes);
        log.info("Produced a message of {} characters", bigMessage.length());
        return bigMessage;
    }

    /**
     * Generates big content with Lorem ipsum paragraph repeated as many times as necessary to get the requested size.
     */
    private String generateBigLoremIpsum(int nbBytes) {
        String loremIpsumParagraph = new LoremIpsum().getParagraphs(1) + "\n\n";
        byte[] loremIpsumBytes = loremIpsumParagraph.getBytes(StandardCharsets.UTF_8);
        byte[] bigMessageBytes = new byte[nbBytes];

        // copying as many times as necessary to fill up the whole destination array
        int bytesCovered = 0;
        while (nbBytes - bytesCovered > loremIpsumBytes.length) {
            System.arraycopy(loremIpsumBytes, 0, bigMessageBytes, bytesCovered, loremIpsumBytes.length);
            bytesCovered += loremIpsumBytes.length;
        }
        System.arraycopy(loremIpsumBytes, 0, bigMessageBytes, bytesCovered, nbBytes - bytesCovered);

        return new String(bigMessageBytes);
    }

    /**
     * Throws as many times a {@link RetryableException}, this generic exception configured to be caught a certain amount
     * of times in the RetryDecorator and the run retried.
     * <p>
     * This method is going to be called: max(nb requested retries, nb allowed retries in config (if different from -1))
     * </p>
     */
    private String throwRetryableException(Matcher retryableMatcher) {
        int nbRetries = Integer.parseInt(retryableMatcher.group("nbRetries"));
        if (retryableExceptionTracker.nbPerformedRetries() < nbRetries) {
            retryableExceptionTracker.incrementNbPerformedRetries();
            throw new RetryableException("Retry nb " + retryableExceptionTracker.nbPerformedRetries() + " of " + nbRetries);
        }
        return "OK";
    }
}
