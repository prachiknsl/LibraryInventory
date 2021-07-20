package com.learnkafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.entity.LibraryEvent;
import com.learnkafka.jpa.LibraryEventsRepository;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.Objects;
import java.util.Optional;

@Service
@Slf4j
public class LibraryEventService {

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    KafkaTemplate kafkaTemplate;

    @Autowired
    private LibraryEventsRepository libraryEventsRepository;

    public void processLibraryEvent(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
        LibraryEvent libraryEvent = objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);
        log.info("LibraryEvent : {}", libraryEvent );

        if(libraryEvent.getLibraryEventId()!=null && libraryEvent.getLibraryEventId()==000)
            throw new RecoverableDataAccessException("Temporary Netwrok issue");
        switch (libraryEvent.getLibraryEventType()){
            case NEW:
                //Save
                save(libraryEvent);
                break;
            case UPDATE:
                validate(libraryEvent);
                save(libraryEvent);
                //update operation
                break;
            default:
                log.info("Invalid Library Event Type");
        }
    }

    private void validate(LibraryEvent libraryEvent) {
        if(Objects.isNull(libraryEvent.getLibraryEventId())){
            log.error("Library EventId is null");
            throw new IllegalArgumentException("Library EventId is missing");
        }
        Optional<LibraryEvent> libraryEventOptional = libraryEventsRepository.findById(libraryEvent.getLibraryEventId());
        if(!libraryEventOptional.isPresent()){
            log.error("Library EventId is not valid");
            throw new IllegalArgumentException("Library EventId is not valid");
        }
        log.info("Validation is successful for {}", libraryEventOptional.get());
    }

    private void save(LibraryEvent libraryEvent) {
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventsRepository.save(libraryEvent);
        log.info("Successfully persisted the library event {}",libraryEvent);
    }

    public void handleRecovery(ConsumerRecord<Integer, String> record){
        Integer key = record.key();
        String message = record.value();
        ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.sendDefault(key, message);

        listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
            @Override
            public void onSuccess(SendResult<Integer, String> result) {
                handleSuccess(key, message, result);
            }

            @SneakyThrows
            @Override
            public void onFailure(Throwable ex) {
                handleFailure(key, message, ex);
            }
        });
    }

    private  void  handleFailure(Integer key, String value, Throwable ex) throws Throwable {
        log.error("Error Sending the message and exception is {}", ex.getMessage());
        try {
            throw ex;
        }catch (Throwable throwable){
            log.error("Error in OnFailure : {}", throwable.getMessage());
        }
    }
    private void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {
        log.info("Message Sent Successfully for the key : {}, value is {} and partition is {}",key, value, result.getRecordMetadata().partition());
    }
}
