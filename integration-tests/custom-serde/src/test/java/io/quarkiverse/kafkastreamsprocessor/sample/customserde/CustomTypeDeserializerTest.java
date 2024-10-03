package io.quarkiverse.kafkastreamsprocessor.sample.customserde;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

class CustomTypeDeserializerTest {

    CustomTypeDeserializer deserializer = new CustomTypeDeserializer(new ObjectMapper());

    @Test
    public void testDeserialize() {
        byte[] data = "{\"value\":11}".getBytes();

        Object customType = deserializer.deserialize("topic", data);

        assertThat(((CustomType) customType).getValue(), equalTo(1));
    }

}