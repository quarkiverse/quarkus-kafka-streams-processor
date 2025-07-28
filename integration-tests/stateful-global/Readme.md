# Kafka Stateful Global with Quarkus

This module demonstrates a stateful microservice using [KafkaStreams](https://kafka.apache.org/documentation/streams/) with a global state store.

## Introduction

The sample implements a KafkaStreams processor that leverages a global state store. Global state stores allow all instances of your application to access the same data, loaded from a dedicated Kafka topic.


## Services showcase

This module demonstrates stateful event processing using the functional processor `PingProcessor` and two global state stores that are registered via the `SampleConfigurationCustomizer` bean, which wires the stores and their processors into the KafkaStreams topology. Here are the two stores:

* store-data: Stores key-value pairs from the Kafka topic using the default processor `DefaultGlobalStateStoreProcessor`.
* store-data-capital: Stores key-value pairs with values automatically capitalized by the custom processor `CapitalizingStoreProcessor` which showcasing how to customize state store behavior

## Implementation note

### Quarkus

The bootstrap of this sample is [Quarkus](https://quarkus.io/)

## Quarkus Dev mode

The sample is fully working with the Quarkus Dev mode that allows to
modify the code and have a hot replacement when the file is saved. It
can be used also to launch the application.

```
$> mvn clean install quarkus:dev
```