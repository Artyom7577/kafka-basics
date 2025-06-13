package com.artyom.kafkapart1.sec04;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;
import reactor.test.StepVerifier;

import java.lang.reflect.Method;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@SpringJUnitConfig
@EmbeddedKafka(partitions = 1, topics = {"order-events"})
class KafkaProducerTest {

    @Mock
    private KafkaSender<String, String> mockSender;

    private Map<String, Object> producerConfig;

    @BeforeEach
    void setUp() {
        producerConfig = Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        );
    }

    @Test
    void testProducerConfiguration() {
        // Test that the producer configuration contains expected values
        assertNotNull(producerConfig);
        assertEquals("localhost:9092", producerConfig.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals(StringSerializer.class, producerConfig.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
        assertEquals(StringSerializer.class, producerConfig.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
    }

    @Test
    void testCreateSenderRecord() throws Exception {
        // Use reflection to access the private method
        Method createSenderRecordMethod = KafkaProducer.class.getDeclaredMethod("createSenderRecord", Integer.class);
        createSenderRecordMethod.setAccessible(true);

        // Test record creation for different values
        for (int i = 1; i <= 5; i++) {
            SenderRecord<String, String, String> record = 
                (SenderRecord<String, String, String>) createSenderRecordMethod.invoke(null, i);

            assertNotNull(record);
            assertEquals("order-events", record.topic());
            assertNull(record.partition()); // partition is null as per the original code
            assertEquals(String.valueOf(i), record.key());
            assertEquals("order-" + i, record.value());
            assertEquals(String.valueOf(i), record.correlationMetadata());

            // Test headers
            RecordHeaders headers = (RecordHeaders) record.headers();
            assertNotNull(headers);
            
            Header clientIdHeader = headers.lastHeader("client-id");
            assertNotNull(clientIdHeader);
            assertEquals("some-client", new String(clientIdHeader.value()));

            Header tracingIdHeader = headers.lastHeader("tracing-id");
            assertNotNull(tracingIdHeader);
            assertEquals("123456", new String(tracingIdHeader.value()));
        }
    }

    @Test
    void testFluxRangeCreation() {
        // Test that Flux.range creates the expected number of elements
        Flux<Integer> flux = Flux.range(1, 10);
        
        StepVerifier.create(flux)
            .expectNextCount(10)
            .verifyComplete();
    }

    @Test
    void testSenderRecordCreationWithReflection() throws Exception {
        // Test the createSenderRecord method using reflection
        Method createSenderRecordMethod = KafkaProducer.class.getDeclaredMethod("createSenderRecord", Integer.class);
        createSenderRecordMethod.setAccessible(true);

        Integer testValue = 42;
        SenderRecord<String, String, String> record = 
            (SenderRecord<String, String, String>) createSenderRecordMethod.invoke(null, testValue);

        // Verify the record structure
        assertNotNull(record);
        assertEquals("order-events", record.topic());
        assertEquals("42", record.key());
        assertEquals("order-42", record.value());
        assertEquals("42", record.correlationMetadata());

        // Verify headers
        RecordHeaders headers = (RecordHeaders) record.headers();
        assertEquals("some-client", new String(headers.lastHeader("client-id").value()));
        assertEquals("123456", new String(headers.lastHeader("tracing-id").value()));
    }

    @Test
    void testProducerRecordStructure() throws Exception {
        // Test the underlying ProducerRecord structure
        Method createSenderRecordMethod = KafkaProducer.class.getDeclaredMethod("createSenderRecord", Integer.class);
        createSenderRecordMethod.setAccessible(true);

        SenderRecord<String, String, String> senderRecord = 
            (SenderRecord<String, String, String>) createSenderRecordMethod.invoke(null, 1);

        assertNotNull(senderRecord);
        assertEquals("order-events", senderRecord.topic());
        assertNull(senderRecord.partition());
        assertEquals("1", senderRecord.key());
        assertEquals("order-1", senderRecord.value());
        assertNotNull(senderRecord.headers());
    }

    @Test
    void testHeaderValues() throws Exception {
        // Test that headers contain the expected values
        Method createSenderRecordMethod = KafkaProducer.class.getDeclaredMethod("createSenderRecord", Integer.class);
        createSenderRecordMethod.setAccessible(true);

        SenderRecord<String, String, String> record = 
            (SenderRecord<String, String, String>) createSenderRecordMethod.invoke(null, 1);

        RecordHeaders headers = (RecordHeaders) record.headers();
        
        // Test client-id header
        Header clientIdHeader = headers.lastHeader("client-id");
        assertNotNull(clientIdHeader);
        assertEquals("some-client", new String(clientIdHeader.value()));

        // Test tracing-id header
        Header tracingIdHeader = headers.lastHeader("tracing-id");
        assertNotNull(tracingIdHeader);
        assertEquals("123456", new String(tracingIdHeader.value()));

        // Test that headers are immutable (should not be null)
        assertNotNull(headers.toArray());
        assertTrue(headers.toArray().length > 0);
    }

    @Test
    void testCorrelationMetadata() throws Exception {
        // Test that correlation metadata matches the key
        Method createSenderRecordMethod = KafkaProducer.class.getDeclaredMethod("createSenderRecord", Integer.class);
        createSenderRecordMethod.setAccessible(true);

        for (int i = 1; i <= 10; i++) {
            SenderRecord<String, String, String> record = 
                (SenderRecord<String, String, String>) createSenderRecordMethod.invoke(null, i);

            assertEquals(String.valueOf(i), record.correlationMetadata());
            assertEquals(record.key(), record.correlationMetadata());
        }
    }

    @Test
    void testNullPartition() throws Exception {
        // Test that partition is null as specified in the original code
        Method createSenderRecordMethod = KafkaProducer.class.getDeclaredMethod("createSenderRecord", Integer.class);
        createSenderRecordMethod.setAccessible(true);

        SenderRecord<String, String, String> record = 
            (SenderRecord<String, String, String>) createSenderRecordMethod.invoke(null, 1);

        assertNull(record.partition());
    }

    @Test
    void testTopicName() throws Exception {
        // Test that the topic name is correct
        Method createSenderRecordMethod = KafkaProducer.class.getDeclaredMethod("createSenderRecord", Integer.class);
        createSenderRecordMethod.setAccessible(true);

        SenderRecord<String, String, String> record = 
            (SenderRecord<String, String, String>) createSenderRecordMethod.invoke(null, 1);

        assertEquals("order-events", record.topic());
    }
} 