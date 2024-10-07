package io.quarkiverse.kafkastreamsprocessor.sample.customserde;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.databind.ObjectMapper;

@ApplicationScoped
public class CustomTypeSerializer implements Serializer<CustomType> {
    private final ObjectMapper objectMapper;

    @Inject
    public CustomTypeSerializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public byte[] serialize(String topic, CustomType data) {
        CustomType valueToSerialize = new CustomType(data.getValue() + CustomTypeSerde.SHIFT);
        try {
            return objectMapper.writeValueAsBytes(valueToSerialize);
        } catch (Exception e) {
            throw new RuntimeException("Error serializing CustomType", e);
        }
    }

}
