package io.quarkiverse.kafkastreamsprocessor.sample.customserde;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class CustomType {
    private int value;
}
