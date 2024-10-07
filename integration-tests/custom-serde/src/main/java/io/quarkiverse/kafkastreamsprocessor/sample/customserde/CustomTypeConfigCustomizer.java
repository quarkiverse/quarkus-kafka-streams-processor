package io.quarkiverse.kafkastreamsprocessor.sample.customserde;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;

import io.quarkiverse.kafkastreamsprocessor.api.configuration.Configuration;
import io.quarkiverse.kafkastreamsprocessor.api.configuration.ConfigurationCustomizer;

@Dependent
@Priority(1)
public class CustomTypeConfigCustomizer implements ConfigurationCustomizer {
    private final CustomTypeSerde serde;
    private final CustomTypeSerializer serializer;

    @Inject
    public CustomTypeConfigCustomizer(CustomTypeSerde serde, CustomTypeSerializer serializer) {
        this.serde = serde;
        this.serializer = serializer;
    }

    @Override
    public void fillConfiguration(Configuration configuration) {
        configuration.setSourceValueSerde(serde);
        configuration.setSinkValueSerializer(serializer);
    }
}
