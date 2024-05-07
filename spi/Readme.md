# SPI

SPI module for the Kafka Streams Processor library.
It is expected to be put as a compile dependency on the application's classpath.

## Smallrye Config properties

With the `spi` module, you can inject the configuration mapping this Quarkus extension uses.
Namely the `KStreamsProcessorConfig` class.

The configuration is of scope `RUNTIME`, so do not inject it in a custom Quarkus extension's build time `BuildStep` method.
The consequence would be that Quarkus would consider now this configuration as build time, and not allow you to override it at runtime.

## Topic mappings

There might still be usecases where you would want to have access to the default topics that are configured by default for each source and sink.
The `spi` module grants you this access through alternative constructors to the `SinkToTopicMappingBuilder` and `SourceToTopicsMappingBuilder` classes taking a `org.eclipse.microprofile.config.Config` object as parameter.

Example of usage:

```java
class UsageExample {
  public void usageExample(Config config) {
    Map<String, String> sinkMapping = new SinkToTopicMappingBuilder(config).sinkToTopicMapping();
    Map<String, String[]> sourceMapping = new SourceToTopicsMappingBuilder(config).sourceToTopicsMapping();
  }
}
```