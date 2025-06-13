package com.artyom.kafkapart1.sec04;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;
import reactor.test.StepVerifier;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@SpringJUnitConfig
@EmbeddedKafka(partitions = 1, topics = {"order-events"}, bootstrapServersProperty = "spring.kafka.bootstrap-servers")
class KafkaProducerIntegrationTest {
    private KafkaSender<String, String> sender;
    private KafkaConsumer<String, String> consumer;
    private static final String TOPIC = "order-events";

    @BeforeEach
    void setUp() {
        // Producer configuration
        Map<String, Object> producerConfig = Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        );

        SenderOptions<String, String> options = SenderOptions.create(producerConfig);
        sender = KafkaSender.create(options);

        // Consumer configuration
        Map<String, Object> consumerConfig = Map.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
            ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group",
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class
        );

        consumer = new KafkaConsumer<>(consumerConfig);
        consumer.subscribe(Collections.singletonList(TOPIC));
    }

    @AfterEach
    void tearDown() {
        if (sender != null) {
            sender.close();
        }
        if (consumer != null) {
            consumer.close();
        }
    }

    @Test
    void testProducerSendsMessagesToKafka() throws Exception {
        // Create a flux of 5 messages
        Flux<SenderRecord<String, String, String>> flux = Flux.range(1, 5)
            .map(this::createSenderRecord);

        // Send messages
        CountDownLatch latch = new CountDownLatch(5);
        
        sender.send(flux)
            .doOnNext(result -> {
                String key = result.correlationMetadata();
                System.out.println("Sent message with correlation id: " + key);
                latch.countDown();
            })
            .doOnComplete(() -> System.out.println("All messages sent"))
            .subscribe();

        // Wait for messages to be sent
        assertTrue(latch.await(10, TimeUnit.SECONDS), "Messages should be sent within 10 seconds");

        // Consume messages and verify
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
        
        assertFalse(records.isEmpty(), "Should receive messages from Kafka");
        assertEquals(5, records.count(), "Should receive exactly 5 messages");

        // Verify each message
        for (ConsumerRecord<String, String> record : records) {
            assertNotNull(record.key());
            assertNotNull(record.value());
            assertTrue(record.value().startsWith("order-"));
            assertEquals(TOPIC, record.topic());
            
            // Verify headers
            assertNotNull(record.headers());
            assertTrue(record.headers().toArray().length > 0);
        }
    }

    @Test
    void testReactiveStreamCompletesSuccessfully() throws Exception {
        // Test that the reactive stream completes without errors
        Flux<SenderRecord<String, String, String>> flux = Flux.range(1, 3)
            .map(this::createSenderRecord);

        StepVerifier.create(sender.send(flux))
            .expectNextCount(3)
            .verifyComplete();
    }

    @Test
    void testMessageHeadersArePreserved() throws Exception {
        // Send a single message
        Flux<SenderRecord<String, String, String>> flux = Flux.just(createSenderRecord(1));

        CountDownLatch latch = new CountDownLatch(1);
        
        sender.send(flux)
            .doOnNext(result -> latch.countDown())
            .subscribe();

        assertTrue(latch.await(5, TimeUnit.SECONDS));

        // Consume and verify headers
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
        assertFalse(records.isEmpty());

        ConsumerRecord<String, String> record = records.iterator().next();
        
        // Verify headers
        var clientIdHeader = record.headers().lastHeader("client-id");
        assertNotNull(clientIdHeader);
        assertEquals("some-client", new String(clientIdHeader.value()));

        var tracingIdHeader = record.headers().lastHeader("tracing-id");
        assertNotNull(tracingIdHeader);
        assertEquals("123456", new String(tracingIdHeader.value()));
    }

    @Test
    void testMultipleMessagesWithDifferentKeys() throws Exception {
        // Send messages with different keys
        Flux<SenderRecord<String, String, String>> flux = Flux.range(1, 10)
            .map(this::createSenderRecord);

        CountDownLatch latch = new CountDownLatch(10);
        
        sender.send(flux)
            .doOnNext(result -> latch.countDown())
            .subscribe();

        assertTrue(latch.await(10, TimeUnit.SECONDS));

        // Consume and verify all messages
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
        assertEquals(10, records.count());

        // Verify keys are unique
        Set<String> uniqueKeys = new HashSet<>();
        for (ConsumerRecord<String, String> record : records) {
            uniqueKeys.add(record.key());
        }
        assertEquals(10, uniqueKeys.size(), "All keys should be unique");
    }

    private SenderRecord<String, String, String> createSenderRecord(Integer i) {
        try {
            Method createSenderRecordMethod = KafkaProducer.class.getDeclaredMethod("createSenderRecord", Integer.class);
            createSenderRecordMethod.setAccessible(true);
            return (SenderRecord<String, String, String>) createSenderRecordMethod.invoke(null, i);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create sender record", e);
        }
    }
} 