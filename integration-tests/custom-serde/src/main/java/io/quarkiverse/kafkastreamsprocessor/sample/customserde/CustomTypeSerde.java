package io.quarkiverse.kafkastreamsprocessor.sample.customserde;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

@ApplicationScoped
public class CustomTypeSerde implements Serde<CustomType> {
    static final int SHIFT = 10;

    private final CustomTypeSerializer customTypeSerializer;

    private final CustomTypeDeserializer customTypeDeserializer;

    @Inject
    public CustomTypeSerde(CustomTypeSerializer customTypeSerializer, CustomTypeDeserializer customTypeDeserializer) {
        this.customTypeSerializer = customTypeSerializer;
        this.customTypeDeserializer = customTypeDeserializer;
    }

    @Override
    public Serializer<CustomType> serializer() {
        return customTypeSerializer;
    }

    @Override
    public Deserializer<CustomType> deserializer() {
        return customTypeDeserializer;
    }
}
