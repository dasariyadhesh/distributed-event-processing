package com.eventprocessing.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Event {

    private String eventId;
    private String eventType;
    private String payload;
    private String source;
    private int retryCount;
    private EventStatus status;

    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
    private LocalDateTime createdAt;

    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
    private LocalDateTime processedAt;

    public static Event of(String eventType, String payload, String source) {
        return Event.builder()
                .eventId(UUID.randomUUID().toString())
                .eventType(eventType)
                .payload(payload)
                .source(source)
                .retryCount(0)
                .status(EventStatus.PENDING)
                .createdAt(LocalDateTime.now())
                .build();
    }

    public enum EventStatus {
        PENDING, PROCESSING, COMPLETED, FAILED, DEAD_LETTERED
    }
}
