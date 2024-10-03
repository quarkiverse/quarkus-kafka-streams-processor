package io.quarkiverse.kafkastreamsprocessor.sample.customserde;

import java.nio.charset.StandardCharsets;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.apache.kafka.common.serialization.Deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

@ApplicationScoped
@Slf4j
public class CustomTypeDeserializer implements Deserializer<CustomType> {
    private final ObjectMapper objectMapper;

    @Inject
    public CustomTypeDeserializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public CustomType deserialize(String topic, byte[] data) {
        try {
            CustomType readValue = objectMapper.readValue(data, CustomType.class);
            return new CustomType(readValue.getValue() - CustomTypeSerde.SHIFT);
        } catch (Exception e) {
            log.error("Could not deserialize: {}", new String(data, StandardCharsets.UTF_8));
            throw new RuntimeException("Error deserializing CustomType", e);
        }
    }
}
