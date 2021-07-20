package com.learnkafka.intg;

import com.learnkafka.domain.Book;
import com.learnkafka.domain.LibraryEvent;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(topics = {"library-events"}, partitions = 3)
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.admin.properties.bootstrap-servers=${spring.embedded.kafka.brokers}"})
public class LibraryEventControllerIT {

    @Autowired
    TestRestTemplate restTemplate;

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    private Consumer<Integer, String> consumer;

    @BeforeEach
    void setUp() {
        Map<String, Object> configs = new HashMap<>(KafkaTestUtils.consumerProps("group1", "true", embeddedKafkaBroker));
        //noinspection unchecked
        consumer = new DefaultKafkaConsumerFactory<Integer, String>(configs).createConsumer();
        embeddedKafkaBroker.consumeFromAllEmbeddedTopics(consumer);
    }

    @AfterEach
    void tearDown() {
        consumer.close();
    }


    @Test
    void postLibraryEvent() throws InterruptedException {
        Book book = Book.builder()
                .bookId(123)
                .bookAuthor("Prachi")
                .bookName("Kafka using Spring Boot").build();
        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book)
                .build();
        HttpHeaders headers = new HttpHeaders();
        headers.set("content-type", MediaType.APPLICATION_JSON.toString());
        HttpEntity<LibraryEvent> request = new HttpEntity<>(libraryEvent, headers);
        ResponseEntity<LibraryEvent> responseEntity = restTemplate.exchange("/v1/libraryevent", HttpMethod.POST, request, LibraryEvent.class);

        assertEquals(HttpStatus.CREATED, responseEntity.getStatusCode());

        ConsumerRecord<Integer, String> consumerRecord = KafkaTestUtils.getSingleRecord(consumer, "library-events");
        Thread.sleep(3000);
        String expectedRecord = "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":123,\"bookName\":\"Kafka using Spring Boot\",\"bookAuthor\":\"Prachi\"}}";
        String value = consumerRecord.value();
        assertEquals(expectedRecord, value);
    }

    @Test
    void updateLibraryEvent_success() throws InterruptedException {
        Book book = Book.builder()
                .bookId(123)
                .bookAuthor("Prachi")
                .bookName("Kafka using Spring Boot").build();
        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(123)
                .book(book)
                .build();
        HttpHeaders headers = new HttpHeaders();
        headers.set("content-type", MediaType.APPLICATION_JSON.toString());
        HttpEntity<LibraryEvent> request = new HttpEntity<>(libraryEvent, headers);
        ResponseEntity<LibraryEvent> responseEntity = restTemplate.exchange("/v1/libraryevent", HttpMethod.PUT, request, LibraryEvent.class);

        assertEquals(HttpStatus.OK, responseEntity.getStatusCode());

        ConsumerRecord<Integer, String> consumerRecord = KafkaTestUtils.getSingleRecord(consumer, "library-events");
        Thread.sleep(3000);
        String expectedRecord = "{\"libraryEventId\":123,\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":123,\"bookName\":\"Kafka using Spring Boot\",\"bookAuthor\":\"Prachi\"}}";
        String value = consumerRecord.value();
        assertEquals(expectedRecord, value);
    }

    @Test
    void updateLibraryEvent_Failure() throws InterruptedException {
        Book book = Book.builder()
                .bookId(123)
                .bookAuthor("Prachi")
                .bookName("Kafka using Spring Boot").build();
        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book)
                .build();
        HttpHeaders headers = new HttpHeaders();
        headers.set("content-type", MediaType.APPLICATION_JSON.toString());
        HttpEntity<LibraryEvent> request = new HttpEntity<>(libraryEvent, headers);
        ResponseEntity<String> responseEntity = restTemplate.exchange("/v1/libraryevent", HttpMethod.PUT, request, String.class);

        assertEquals(HttpStatus.BAD_REQUEST, responseEntity.getStatusCode());
        assertEquals(responseEntity.getBody(), "Please enter eventID");
    }
}
