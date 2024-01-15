# Text map accessors

Small library proposing getter and setter to use with OpenTelemetry's [W3CTraceContextPropagator](https://github.com/open-telemetry/opentelemetry-java/blob/main/api/all/src/main/java/io/opentelemetry/api/trace/propagation/W3CTraceContextPropagator.java) to extract the propagated trace information (parent trace id and parent span id).