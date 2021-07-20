package com.learnkafka.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.LibraryEvent;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
@Slf4j    //logger instance automatically for us
public class LibraryEventProducer {

    @Autowired
    KafkaTemplate<Integer, String> kafkaTemplate;

    String topic = "library-events";
    @Autowired
    ObjectMapper objectMapper;

    public void sendLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException {
        Integer key = libraryEvent.getLibraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);

        ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.sendDefault(key, value);
        // here call is async not wait for handle success or failure scenario and return to controller
        listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
            @Override
            public void onSuccess(SendResult<Integer, String> result) {
                handleSuccess(key, value, result);
            }

            @SneakyThrows
            @Override
            public void onFailure(Throwable ex) {
                handleFailure(key, value, ex);
            }
        });
    }

    public ListenableFuture<SendResult<Integer, String>> sendLibraryEventApproach3(LibraryEvent libraryEvent) throws JsonProcessingException {
        Integer key = libraryEvent.getLibraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);

        ProducerRecord<Integer, String> producerRecord = buildProducerRecord(key, value, topic);
        ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.send(producerRecord);
        // here call is async not wait for handle success or failure scenario and return to controller
        listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
            @Override
            public void onSuccess(SendResult<Integer, String> result) {
                handleSuccess(key, value, result);
            }

            @SneakyThrows
            @Override
            public void onFailure(Throwable ex) {
                handleFailure(key, value, ex);
            }
        });
        return listenableFuture;
    }

    public SendResult<Integer, String> sendLibraryEventSynchronousApproach2(LibraryEvent libraryEvent) throws JsonProcessingException, InterruptedException, ExecutionException, TimeoutException {
        Integer key = libraryEvent.getLibraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);
        SendResult<Integer, String> sendResult = null;
        try {
            sendResult = kafkaTemplate.sendDefault(key, value).get(1, TimeUnit.SECONDS);
        } catch (ExecutionException | InterruptedException e) {
            log.error("ExecutionException/InterruptedException in Sending the message and exception is {}", e.getMessage());
            throw e;
        } catch (Exception e) {
            log.error("Exception in Sending the message and exception is {}", e.getMessage());
            throw e;
        }
        return sendResult;
    }

    private ProducerRecord<Integer, String> buildProducerRecord(Integer key, String value, String topic){

        List<Header> recordHeaders = List.of(new RecordHeader("event-source", "scanner".getBytes()));
        return new ProducerRecord<Integer, String>(topic, null, key, value, recordHeaders);
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
