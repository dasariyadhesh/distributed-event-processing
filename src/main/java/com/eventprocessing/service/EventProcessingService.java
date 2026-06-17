package com.eventprocessing.service;

import com.eventprocessing.model.Event;
import com.eventprocessing.retry.RetryPolicy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Slf4j
@Service
@RequiredArgsConstructor
public class EventProcessingService {

    private final RetryPolicy retryPolicy;

    /**
     * Core event processing logic. Delegates to type-specific handlers.
     * On failure, applies retry policy with exponential backoff.
     */
    public void process(Event event) {
        log.info("Processing event [{}] of type [{}]", event.getEventId(), event.getEventType());
        event.setStatus(Event.EventStatus.PROCESSING);

        try {
            switch (event.getEventType().toUpperCase()) {
                case "ORDER_CREATED", "ORDER_UPDATED", "ORDER_CANCELLED" -> handleOrderEvent(event);
                case "PAYMENT_INITIATED", "PAYMENT_CONFIRMED", "PAYMENT_FAILED" -> handlePaymentEvent(event);
                case "NOTIFICATION_EMAIL", "NOTIFICATION_SMS" -> handleNotificationEvent(event);
                default -> log.warn("Unknown event type [{}], skipping", event.getEventType());
            }

            event.setStatus(Event.EventStatus.COMPLETED);
            event.setProcessedAt(LocalDateTime.now());
            log.info("Event [{}] processed successfully", event.getEventId());

        } catch (Exception ex) {
            log.error("Error processing event [{}]: {}", event.getEventId(), ex.getMessage());
            event.setStatus(Event.EventStatus.FAILED);
            retryPolicy.handleFailure(event, ex);
        }
    }

    private void handleOrderEvent(Event event) {
        log.info("Handling order event [{}]: {}", event.getEventType(), event.getPayload());
        // Simulate order processing logic
        validatePayload(event.getPayload());
    }

    private void handlePaymentEvent(Event event) {
        log.info("Handling payment event [{}]: {}", event.getEventType(), event.getPayload());
        // Simulate payment processing logic
        validatePayload(event.getPayload());
    }

    private void handleNotificationEvent(Event event) {
        log.info("Handling notification event [{}]: {}", event.getEventType(), event.getPayload());
        // Simulate notification dispatch logic
        validatePayload(event.getPayload());
    }

    private void validatePayload(String payload) {
        if (payload == null || payload.isBlank()) {
            throw new IllegalArgumentException("Event payload cannot be null or empty");
        }
    }
}
