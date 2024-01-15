# kafka-to-rest application

EDA to SOA microservice implementation using [KafkaStreams](https://kafka.apache.org/documentation/streams/) and [microprofile-rest-client](https://github.com/eclipse/microprofile-rest-client)

## End-to-end testing

* Start mock service in one terminal

```
mvn mockserver:run
```

* Start microservice in dev mode in another terminal

```
mvn quarkus:dev
```

* Go to quarkus [dev UI](http://localhost:8080/q/dev-ui/io.quarkus.quarkus-kafka-client/topics)
* Create a new message in `pong-events` using the `+` button.
