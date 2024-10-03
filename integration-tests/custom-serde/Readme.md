# Sample with multiple TopologyConfigCustomizers

EDA to EDA stateless microservice implementation using [KafkaStreams](https://kafka.apache.org/documentation/streams/)

## Introduction

This module showcases the implementation of a
[KafkaStream processor](https://kafka.apache.org/25/documentation/streams/developer-guide/processor-api.html#overview) with multiple [ConfigurationCustomizer](../../api/src/main/java/io/quarkiverse/kafkastreamsprocessor/api/configuration/ConfigurationCustomizer.java) instances.

## Quarkus Dev mode

The sample is fully working with the Quarkus Dev mode that allows to
modify the code and have a hot replacement when the file is saved. It
can be used also to launch the application.

```
$> mvn clean install quarkus:dev
```
