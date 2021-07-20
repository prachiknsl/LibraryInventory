package com.learnkafka.unit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.controller.LibraryEventsController;
import com.learnkafka.domain.Book;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.producer.LibraryEventProducer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(LibraryEventsController.class)
@AutoConfigureMockMvc
public class LibraryEventControllerUT {

    @Autowired
    MockMvc mockMvc;

    ObjectMapper objectMapper = new ObjectMapper();

    @MockBean
    LibraryEventProducer libraryEventProducer;

    @Test
    void postLibraryEvent() throws Exception {
        Book book = Book.builder()
                .bookId(123)
                .bookAuthor("Prachi")
                .bookName("Kafka using Spring Boot").build();
        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book)
                .build();

        String json = objectMapper.writeValueAsString(libraryEvent);

        when(libraryEventProducer.sendLibraryEventApproach3(isA(LibraryEvent.class))).thenReturn(null);
        //When
        mockMvc.perform(post("/v1/libraryevent")
        .content(json)
        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isCreated());
        //Then
    }

    @Test
    void postLibraryEvent_400() throws Exception {
        Book book = Book.builder()
                .bookId(null)
                .bookAuthor(null)
                .bookName("Kafka using Spring Boot").build();
        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book)
                .build();

        String json = objectMapper.writeValueAsString(libraryEvent);

        //doNothing().when(libraryEventProducer).sendLibraryEventApproach3(libraryEvent);
        when(libraryEventProducer.sendLibraryEventApproach3(isA(LibraryEvent.class))).thenReturn(null);
        //When
        String expectedError = "book.bookAuthor - must not be blank, book.bookId - must not be null";
        mockMvc.perform(post("/v1/libraryevent")
                .content(json)
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().is4xxClientError())
        .andExpect(content().string(expectedError));
    }

    @Test
    void updateLibraryEvent_success() throws Exception {
        Book book = Book.builder()
                .bookId(123)
                .bookAuthor("Prachi")
                .bookName("Kafka using Spring Boot").build();
        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(1234)
                .book(book)
                .build();

        String json = objectMapper.writeValueAsString(libraryEvent);

        when(libraryEventProducer.sendLibraryEventApproach3(isA(LibraryEvent.class))).thenReturn(null);
        //When
        mockMvc.perform(put("/v1/libraryevent")
                .content(json)
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk());
        //Then
    }

    @Test
    void updateLibraryEvent_400() throws Exception {
        Book book = Book.builder()
                .bookId(123)
                .bookAuthor("Prachi")
                .bookName("Kafka using Spring Boot").build();
        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book)
                .build();

        String json = objectMapper.writeValueAsString(libraryEvent);

        //doNothing().when(libraryEventProducer).sendLibraryEventApproach3(libraryEvent);
        when(libraryEventProducer.sendLibraryEventApproach3(isA(LibraryEvent.class))).thenReturn(null);
        //When
        String expectedError = "Please enter eventID";
        mockMvc.perform(put("/v1/libraryevent")
                .content(json)
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().is4xxClientError())
                .andExpect(content().string(expectedError));
    }
}
