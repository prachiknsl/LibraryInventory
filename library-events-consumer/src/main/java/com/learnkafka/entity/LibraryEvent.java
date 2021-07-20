package com.learnkafka.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
@Entity
public class LibraryEvent {

    @Id
    @GeneratedValue
    private Integer libraryEventId;
    @Enumerated(EnumType.STRING)
    private LibraryEventType libraryEventType;
    @OneToOne(mappedBy = "libraryEvent", cascade = {CascadeType.ALL})
    private Book book;
}
