package io.quarkiverse.kafkastreamsprocessor.sample.customserde;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

class CustomTypeSerializerTest {
    CustomTypeSerializer serializer = new CustomTypeSerializer(new ObjectMapper());

    @Test
    public void testSerialize() {
        CustomType customType = new CustomType(1);

        byte[] serialized = serializer.serialize("topic", customType);

        assertThat(new String(serialized, StandardCharsets.UTF_8), equalTo("{\"value\":11}"));
    }
}