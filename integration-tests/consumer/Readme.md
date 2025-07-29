# Kafka streams processing with Quarkus

EDA consumer microservice implementation using [Kafka Streams](https://kafka.apache.org/documentation/streams/)

## Introduction

This module showcases the implementation of a [Kafka Stream processor](https://kafka.apache.org/25/documentation/streams/developer-guide/processor-api.html#overview) with `Void` output types (`Processor<K, V, Void, Void>`), i.e. a pure consumer.

## Services showcase

This module showcases the stateless processing of `MyData` events and their storage in memory.
The application also exposes a REST API for testing purpose, to query the content of the in-memory storage.

The `io.quarkiverse.kafkastreamsprocessor.sample.consumer.ConsumerProcessor` is associated to a `KafkaStreams topology` that is built using a [CDI producer](https://docs.jboss.org/weld/reference/1.0.0/en-US/html/producermethods.html) backed by the CDI bean `io.quarkiverse.kafkastreamsprocessor.impl.TopologyProducer`

## Implementation note

### Quarkus

The bootstrap of this sample is [Quarkus](https://quarkus.io/)

### Topology driver

## Quarkus Dev mode

The sample is fully working with the Quarkus Dev mode that allows to modify the code and have a hot replacement when the file is saved. 
It can be used also to launch the application.

```
$> mvn clean install quarkus:dev
```
