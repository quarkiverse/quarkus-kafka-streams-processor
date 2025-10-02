# Support cloud events

* Status: drafted
* Deciders: edeweerd1A, csemaan1A, ivantchomgue1a, lmartella1, Annlazar, vietk
* Date: 2025-10-03

Technical Story: issue #219

## Context and Problem Statement

Cloud Events are a specification for describing event data in a common way.
Events are everywhere, yet event publishers tend to describe events differently.
Motivations:

* Consistency: the lack of a common way of describing events means developers have to write new event handling logic for each event source.
* Accessibility: no common event format means no common libraries, tooling, and infrastructure for delivering event data across environments.
* Portability: The portability and productivity we can achieve from event data is hindered overall.

Support cloud events will align this library with those principals.

## Decision Drivers

* Ease of usage by end users
* Standard library usage
* Extensibility

## Considered Options

* Nothing in the library, all is done through configuration customization and type declaration
* Native support by the library, that can be enabled with microprofile-config 

## Decision Outcome

Chosen option: "Native support by the library", because it enhances the library to support natively a data format that is becoming a standard for Event Driven Architecture.

### Consequences

* The library stays up to date with the standard of the industry
* The ProducerOnSendInterceptor decorator scheme has been deprecated since 4.1.
For reference, it allows to intercept records after the payload serialization, just before they are produced to Kafka.
To support CloudEvent with the integrated solution, ProducerOnSendInterceptor will have to be reintroduced, technically removing the @Deprecated annotation.
* The cloud events SDK will be always present on the classpath, even if users do not use cloud events.
An accepted limitation, for it is expected that more and more users change their messages to the `CloudEvent` format.

## Pros and Cons of the Options

### Configuration customization and generic type

[POC](https://github.com/quarkiverse/quarkus-kafka-streams-processor/compare/main...edeweerd1A:quarkus-kafka-streams-processor:cloud-events-generic-type?expand=1) | [diff](../adr/cloud-events-generic-type.patch)

The idea here is to not modify the library.
Each end user to set CloudEvent up through type declaration in the processor, and through serde configuration with a ConfigurationCustomizer.

* Good, because we keep the quarkus-kafka-streams-processor complexity to its current level
* Good, because it leaves complete control to the end user of the serialization/deserialization and metadata/payload manipulations
* Good, because it is based on the opensource and standard cloud events Java SDK
* Bad, because it makes for a lot of boilerplate code that will have to be repeated in every microservice that support cloud events
* Bad, because it won't be possible to write a reusable ProcessorDecorator or OutputRecordInterceptor to streamline processing across a set of microservices

### Native support by the library

[POC](https://github.com/quarkiverse/quarkus-kafka-streams-processor/compare/main...edeweerd1A:quarkus-kafka-streams-processor:cloud-events-integrated?expand=1) | [diff](../adr_copy/cloud-events-integrated.patch)

With this option, users only have to use some microprofile-config configuration.
The library automatically switches to parse incoming messages to `CloudEvent` and, in a second time, deserialize the payload with the configured value deserializer (`Configuration.setSourceValueSerde`) before it is fed to the Processor.
Vice versa, if activated, after the payload has been serialized to `byte[]`, the payload is encapsulated into a `CloudEvent`, that is then serialized for production.
A `CloudEventContextHandler` is provided to access the incoming `CloudEvent` metadata and define the metadata of the outgoing `CloudEvent`.

* Good, because its usage by end users is easy
* Good, because it leaves entire control to end users to define the cloud event metadata before a message is sent
* Good, because it is based on the opensource and standard cloud events Java SDK

## Links

* [Cloud Events Specification](https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/spec.md)
* [Cloud Events Java SDK](https://cloudevents.github.io/sdk-java/)