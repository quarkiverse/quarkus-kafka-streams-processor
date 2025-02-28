package io.quarkiverse.kafkastreamsprocessor.api.decorator.outputrecord;

import org.apache.kafka.streams.processor.api.Record;

/**
 * Interceptor that if implemented is called whenever the processor calls any of
 * {@link org.apache.kafka.streams.processor.api.ProcessorContext}'s forward methods.
 * <p>
 * Order of execution is guaranteed based on the integer priority returned by {@link #priority()}.
 * </p>
 * <p>
 * It differs from {@link io.quarkiverse.kafkastreamsprocessor.api.decorator.producer.ProducerOnSendInterceptor} in that
 * it is executed before any serialization of the payload to byte[] happens.
 * </p>
 */
public interface OutputRecordInterceptor {
    /**
     * By default, if not overriden, the interceptor has the following priority.
     */
    int DEFAULT_PRIORITY = 100;

    /**
     * Override this method to finely tune the order of execution of any interceptor you implement.
     *
     * @return the custom priority you want to assign. A number between 0 and {@link Integer#MAX_VALUE}.
     */
    default int priority() {
        return DEFAULT_PRIORITY;
    }

    /**
     * Intercept the record before it is eventually given to
     * {@link org.apache.kafka.streams.processor.api.ProcessorContext#forward(Record, String)}.
     *
     * @param record the record as the processor requested it to be forwarded
     * @return the new record with any modifications this interceptor wants to apply before serialization.
     */
    Record interceptOutputRecord(Record record);
}
