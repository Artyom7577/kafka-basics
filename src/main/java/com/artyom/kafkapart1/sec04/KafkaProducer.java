package com.artyom.kafkapart1.sec04;

import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
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

        var options = SenderOptions.<String, String>create(producerConfig);

        var flux = Flux.range(1, 10)
            .map(KafkaProducer::createSenderRecord);

        KafkaSender<String, String> sender = KafkaSender.create(options);

        sender.send(flux)
            .doOnNext(result -> {
                String key = result.correlationMetadata();
                log.info("correlation id: {}", key);
            })
            .doOnComplete(sender::close)
            .subscribe();
    }


    private static SenderRecord<String, String, String> createSenderRecord(Integer i) {
        var headers = new RecordHeaders();
        headers.add("client-id", "some-client".getBytes());
        headers.add("tracing-id", "123456".getBytes());
        var pr = new ProducerRecord<>("order-events", null, i.toString(), "order-" + i, headers);
        return SenderRecord.create(pr, pr.key());
    }
}
