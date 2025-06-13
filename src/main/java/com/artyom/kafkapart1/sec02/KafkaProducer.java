package com.artyom.kafkapart1.sec02;

import java.time.Duration;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

public class KafkaProducer {
    private static final Logger log = LoggerFactory.getLogger(KafkaProducer.class);

    public static void main(String[] args) {
        var producerConfig = Map.<String, Object>of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        );

        SenderOptions<String, String> options = SenderOptions.create(producerConfig);

        var flux = Flux.interval(Duration.ofMillis(1000))
            .take(100)
            .map(i -> new ProducerRecord<>("order-events", i.toString(), "order-" + i))
            .map(record -> SenderRecord.create(record, record.key()));

        KafkaSender<String, String> sender = KafkaSender.create(options);

        sender
            .send(flux)
            .doOnNext(result -> {
                String key = result.correlationMetadata();
                log.info("correlation id: {}", key);
            })
            .doOnComplete(sender::close)
            .subscribe();
    }
}
