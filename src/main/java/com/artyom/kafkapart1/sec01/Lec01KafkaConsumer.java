package com.artyom.kafkapart1.sec01;

import java.util.Map;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

/*
    to demo a simple kafka consumer using reactor kafka

    producer ---> kafka broker <----> consumer

    topic: order-events
    partitions: 1
    long-end-offset: 15
    current-offset: 0
 */
public class Lec01KafkaConsumer {
    private static final Logger log = LoggerFactory.getLogger(Lec01KafkaConsumer.class);

    public static void main(String[] args) {

        var consumerConfig = Map.<String, Object>of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
            ConsumerConfig.GROUP_ID_CONFIG, "demo-group",
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
            ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "1"
        );

        var options = ReceiverOptions.create(consumerConfig)
            .subscription(Pattern.compile("order.*"));

        KafkaReceiver.create(options)
            .receive()

            .doOnNext(r -> log.info("Received: topic: {}, key: {}, value: {}", r.topic(), r.key(), r.value()))
            .doOnNext(r -> r.receiverOffset().acknowledge()) // offset will be 15 if 15 items consumes next time we will not see that
            .subscribe();
    }
}
