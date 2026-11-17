package io.quarkiverse.kafkastreamsprocessor.impl.decorator.processor;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.time.Duration;
import java.util.Set;

import jakarta.enterprise.inject.Alternative;
import jakarta.inject.Inject;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.github.daniel.shuy.kafka.protobuf.serde.KafkaProtobufDeserializer;
import com.github.daniel.shuy.kafka.protobuf.serde.KafkaProtobufSerializer;

import io.quarkiverse.kafkastreamsprocessor.api.Processor;
import io.quarkiverse.kafkastreamsprocessor.sample.message.PingMessage;
import io.quarkiverse.kafkastreamsprocessor.spi.properties.KStreamsProcessorConfig;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.kafka.InjectKafkaCompanion;
import io.quarkus.test.kafka.KafkaCompanionResource;
import io.smallrye.common.vertx.ContextLocals;
import io.smallrye.common.vertx.VertxContext;
import io.smallrye.reactive.messaging.kafka.companion.ConsumerBuilder;
import io.smallrye.reactive.messaging.kafka.companion.KafkaCompanion;
import io.smallrye.reactive.messaging.kafka.companion.ProducerBuilder;
import io.vertx.core.Vertx;

@QuarkusTest
@TestProfile(VertxContextDecoratorQuarkusTest.VertxContextTestProfile.class)
@QuarkusTestResource(KafkaCompanionResource.class)
public class VertxContextDecoratorQuarkusTest {

    @Inject
    KStreamsProcessorConfig kStreamsProcessorConfig;

    @InjectKafkaCompanion
    KafkaCompanion kafkaCompanion;

    ProducerBuilder<String, PingMessage.Ping> producer;

    ConsumerBuilder<String, PingMessage.Ping> consumer;

    @BeforeEach
    public void setup() {
        producer = kafkaCompanion.produceWithSerializers(new StringSerializer(), new KafkaProtobufSerializer<>());
        consumer = kafkaCompanion.consumeWithDeserializers(new StringDeserializer(),
                new KafkaProtobufDeserializer<>(PingMessage.Ping.parser()))
                .withGroupId("test")
                .withOffsetReset(OffsetResetStrategy.EARLIEST.toString())
                .withAutoCommit();
    }

    @AfterEach
    public void tearDown() {
        producer.close();
        consumer.close();
    }

    @Test
    public void shouldRunProcessorInVertxDuplicatedContext() {
        PingMessage.Ping ping = PingMessage.Ping.newBuilder().setMessage("world").build();

        producer.fromRecords(new ProducerRecord<>(kStreamsProcessorConfig.input().topic().get(), ping))
                .awaitCompletion(Duration.ofSeconds(1));

        ConsumerRecord<String, PingMessage.Ping> consumerRecord = consumer
                .fromTopics(kStreamsProcessorConfig.output().topic().get(), 1)
                .awaitCompletion(Duration.ofSeconds(10)).getFirstRecord();
    }

    @Processor
    @Alternative
    public static class TestProcessor extends ContextualProcessor<String, PingMessage.Ping, String, PingMessage.Ping> {

        @Override
        public void process(Record<String, PingMessage.Ping> record) {
            assertThat(Vertx.currentContext(), notNullValue());
            assertThat(VertxContext.isDuplicatedContext(Vertx.currentContext()), is(true));
            assertDoesNotThrow(() -> ContextLocals.put("key", "value"));
            context().forward(record);
        }
    }

    public static class VertxContextTestProfile implements QuarkusTestProfile {
        @Override
        public Set<Class<?>> getEnabledAlternatives() {
            return Set.of(TestProcessor.class);
        }
    }
}
