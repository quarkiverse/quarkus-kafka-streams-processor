# Kafka streams processing with Quarkus

EDA to EDA stateless microservice implementation using [KafkaStreams](https://kafka.apache.org/documentation/streams/)

## Introduction

This module showcases the implementation of a
[KafkaStream processor](https://kafka.apache.org/25/documentation/streams/developer-guide/processor-api.html#overview).

The processor API is one, among other, of the building block of a
KafkaStreams based application, it allows to build a pure EDA
microservice to process streaming of events with an imperative
programming style.

## Services showcase

This module showcases the stateless processing of `Ping` events and
its transformation to `Pong` events. 

The `io.quarkiverse.kafkastreamsprocessor.sample.simple.PingProcessor` is associated to a
`KafkaStreams topology` that is built using a [CDI
producer](https://docs.jboss.org/weld/reference/1.0.0/en-US/html/producermethods.html)
backed by the CDI bean `io.quarkiverse.kafkastreamsprocessor.impl.TopologyProducer`
  
## Implementation note

### Quarkus

The bootstrap of this sample is [Quarkus](https://quarkus.io/)

### Topology driver
 
## Quarkus Dev mode

The sample is fully working with the Quarkus Dev mode that allows to
modify the code and have a hot replacement when the file is saved. It
can be used also to launch the application.

```
$> mvn clean install quarkus:dev
```
