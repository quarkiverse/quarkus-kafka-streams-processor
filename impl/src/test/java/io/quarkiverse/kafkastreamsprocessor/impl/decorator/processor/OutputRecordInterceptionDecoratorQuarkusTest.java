package io.quarkiverse.kafkastreamsprocessor.impl.decorator.processor;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.inject.Inject;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import com.github.daniel.shuy.kafka.protobuf.serde.KafkaProtobufDeserializer;
import com.github.daniel.shuy.kafka.protobuf.serde.KafkaProtobufSerializer;

import io.quarkiverse.kafkastreamsprocessor.api.Processor;
import io.quarkiverse.kafkastreamsprocessor.api.decorator.outputrecord.OutputRecordInterceptor;
import io.quarkiverse.kafkastreamsprocessor.sample.message.PingMessage;
import io.quarkiverse.kafkastreamsprocessor.spi.properties.KStreamsProcessorConfig;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;

@QuarkusTest
@TestProfile(OutputRecordInterceptionDecoratorQuarkusTest.TestProfile.class)
class OutputRecordInterceptionDecoratorQuarkusTest {
    @Inject
    KStreamsProcessorConfig kStreamsProcessorConfig;

    @ConfigProperty(name = "kafka.bootstrap.servers")
    String kafkaBootstrapServers;

    KafkaProducer<String, PingMessage.Ping> producer;

    KafkaConsumer<String, PingMessage.Ping> consumer;

    @BeforeEach
    public void setup() {
        producer = new KafkaProducer<>(KafkaTestUtils.producerProps(kafkaBootstrapServers),
                new StringSerializer(),
                new KafkaProtobufSerializer<>());
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(kafkaBootstrapServers,
                "test", "true");
        consumer = new KafkaConsumer<>(consumerProps, new StringDeserializer(),
                new KafkaProtobufDeserializer<>(PingMessage.Ping.parser()));
        consumer.subscribe(List.of(kStreamsProcessorConfig.output().topic().get()));
    }

    @AfterEach
    public void tearDown() {
        producer.close();
        consumer.close();
    }

    @Test
    void forwardDecoratorCalledInOrder() {
        PingMessage.Ping ping = PingMessage.Ping.newBuilder().setMessage("Hello world").build();
        producer.send(new ProducerRecord<>(kStreamsProcessorConfig.input().topic().get(), 0, "blabla", ping,
                new RecordHeaders().add("header1", "header2".getBytes(StandardCharsets.UTF_8))));
        producer.flush();

        ConsumerRecord<String, PingMessage.Ping> consumerRecord = KafkaTestUtils.getSingleRecord(consumer,
                kStreamsProcessorConfig.output().topic().get(), Duration.ofSeconds(10));

        assertThat(consumerRecord.key(), equalTo("blablase"));
        assertThat(consumerRecord.value().getMessage(), equalTo("Hello worldse"));
        assertThat(new String(consumerRecord.headers().lastHeader("blabla").value(), StandardCharsets.UTF_8),
                equalTo("blibli"));
        assertThat(new String(consumerRecord.headers().lastHeader("blibli").value(), StandardCharsets.UTF_8),
                equalTo("blabla"));
        assertThat(new String(consumerRecord.headers().lastHeader("header1").value(), StandardCharsets.UTF_8),
                equalTo("header2"));
    }

    @Processor
    @Alternative
    public static class TestProcessor extends ContextualProcessor<String, PingMessage.Ping, String, PingMessage.Ping> {
        @Override
        public void process(Record<String, PingMessage.Ping> record) {
            context().forward(record);
        }
    }

    @Alternative
    @ApplicationScoped
    public static class OutputRecordInterceptor1 implements OutputRecordInterceptor {
        @Override
        public Record interceptOutputRecord(Record record) {
            return record.withKey(record.key() + "s")
                    .withValue(PingMessage.Ping.newBuilder().setMessage(((PingMessage.Ping) record.value()).getMessage() + "s")
                            .build())
                    .withHeaders(record.headers().add("blabla", "blibli".getBytes(StandardCharsets.UTF_8)));
        }

        @Override
        public int priority() {
            return 100;
        }
    }

    @Alternative
    @ApplicationScoped
    public static class OutputRecordInterceptor2 implements OutputRecordInterceptor {
        @Override
        public Record interceptOutputRecord(Record record) {
            return record.withKey(record.key() + "e")
                    .withValue(PingMessage.Ping.newBuilder().setMessage(((PingMessage.Ping) record.value()).getMessage() + "e")
                            .build())
                    .withHeaders(record.headers().add("blibli", "blabla".getBytes(StandardCharsets.UTF_8)));
        }

        @Override
        public int priority() {
            return 200;
        }
    }

    public static class TestProfile implements QuarkusTestProfile {
        @Override
        public Set<Class<?>> getEnabledAlternatives() {
            return Set.of(TestProcessor.class, OutputRecordInterceptor1.class, OutputRecordInterceptor2.class);
        }
    }
}
