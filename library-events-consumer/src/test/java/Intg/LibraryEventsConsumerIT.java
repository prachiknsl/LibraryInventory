package Intg;

import ch.qos.logback.core.util.TimeUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.LibraryEventsConsumerApplication;
import com.learnkafka.consumer.LibraryEventsConsumer;
import com.learnkafka.entity.LibraryEvent;
import com.learnkafka.jpa.LibraryEventsRepository;
import com.learnkafka.service.LibraryEventService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.*;

@SpringBootTest(classes = {LibraryEventsConsumerApplication.class})
@EmbeddedKafka(topics = {"library-events"}, partitions = 3)
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}"})
//@TestPropertySource("classpath:application-test.properties")

public class LibraryEventsConsumerIT {

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    KafkaListenerEndpointRegistry endpointRegistry;

    //SpyBean --> Give access to real bean
    @SpyBean
    LibraryEventsConsumer libraryEventsConsumerSpy;

    @SpyBean
    LibraryEventService libraryEventServiceSpy;

    @Autowired
    LibraryEventsRepository libraryEventsRepository;
    @Autowired
    ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        for (MessageListenerContainer messageListenerContainer : endpointRegistry.getListenerContainers()) {
            ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafkaBroker.getPartitionsPerTopic());
        }
    }

    @AfterEach
    void tearDown(){
        libraryEventsRepository.deleteAll();
    }

    @Test
    void publishNewLibraryEvent() throws ExecutionException, InterruptedException, JsonProcessingException {

        String json = "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":123,\"bookName\":\"Kafka using Spring Boot\",\"bookAuthor\":\"Prachi\"}}";
        kafkaTemplate.sendDefault(json).get();

        //CountDownLatch --> block the current excecution of thread and handy for ITs having async calls
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS); //this thread will block for 3 sec, after 3 sec it will executed

        verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));

        List<LibraryEvent> libraryEventList = (List<LibraryEvent>) libraryEventsRepository.findAll();
        assert libraryEventList.size() ==1;
        libraryEventList.forEach(libraryEvent -> {
            assert libraryEvent.getLibraryEventId()!=null;
            assert libraryEvent.getBook().getBookId() == 123;
        });
    }

    @Test
    void publishUpdateLibraryEvent() throws JsonProcessingException, ExecutionException, InterruptedException {
        String json = "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":123,\"bookName\":\"Kafka using Spring Boot\",\"bookAuthor\":\"Prachi\"}}";
        LibraryEvent libraryEvent = objectMapper.readValue(json, LibraryEvent.class);
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventsRepository.save(libraryEvent);
        int eventID = libraryEvent.getLibraryEventId();
        json = "{\"libraryEventId\":"+eventID+",\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":123,\"bookName\":\"Kafka using Spring Boot 2.x\",\"bookAuthor\":\"Prachi\"}}";
        kafkaTemplate.sendDefault(json).get();

        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        LibraryEvent persistedLibraryEvent = libraryEventsRepository.findById(libraryEvent.getLibraryEventId()).get();
        assertEquals( persistedLibraryEvent.getBook().getBookName(), "Kafka using Spring Boot 2.x");
    }

    @Test()
    void publishInvalidLibraryEvent() throws JsonProcessingException, ExecutionException, InterruptedException {
        String json = "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":123,\"bookName\":\"Kafka using Spring Boot\",\"bookAuthor\":\"Prachi\"}}";
        LibraryEvent libraryEvent = objectMapper.readValue(json, LibraryEvent.class);
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventsRepository.save(libraryEvent);
        int eventID = libraryEvent.getLibraryEventId();
        json = "{\"libraryEventId\":"+101+",\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":123,\"bookName\":\"Kafka using Spring Boot 2.x\",\"bookAuthor\":\"Prachi\"}}";
        kafkaTemplate.sendDefault(json).get();

        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);
        verify(libraryEventsConsumerSpy, atLeast(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventServiceSpy, atLeast(1)).processLibraryEvent(isA(ConsumerRecord.class));
        Optional<LibraryEvent> persistedLibraryEvent = libraryEventsRepository.findById(101);
        assertEquals( persistedLibraryEvent.isEmpty(), true);
    }

    @Test
    void publishInvalidLibraryEventType() throws JsonProcessingException, ExecutionException, InterruptedException {
        String json = "{\"libraryEventId\":"+111+",\"libraryEventType\":\"MODIFY\",\"book\":{\"bookId\":123,\"bookName\":\"Kafka using Spring Boot 2.x\",\"bookAuthor\":\"Prachi\"}}";
        kafkaTemplate.sendDefault(json).get();

        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);
        verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));
        Optional<LibraryEvent> persistedLibraryEvent = libraryEventsRepository.findById(101);
        assertEquals( persistedLibraryEvent.isEmpty(), true);
    }

    @Test
    void publishwithSomeNetworkIssue() throws JsonProcessingException, ExecutionException, InterruptedException {
        String json = "{\"libraryEventId\":"+000+",\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":123,\"bookName\":\"Kafka using Spring Boot 2.x\",\"bookAuthor\":\"Prachi\"}}";
        kafkaTemplate.sendDefault(json).get();

        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);
        verify(libraryEventsConsumerSpy, times(4)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventServiceSpy, times(4)).processLibraryEvent(isA(ConsumerRecord.class));
        Optional<LibraryEvent> persistedLibraryEvent = libraryEventsRepository.findById(101);
        assertEquals( persistedLibraryEvent.isEmpty(), true);
    }
}
