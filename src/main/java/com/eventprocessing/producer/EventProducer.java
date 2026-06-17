package com.eventprocessing.producer;

import com.eventprocessing.model.Event;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Slf4j
@Component
@RequiredArgsConstructor
public class EventProducer {

    private final KafkaTemplate<String, Event> kafkaTemplate;

    @Value("${app.kafka.topics.order-events}")
    private String orderEventsTopic;

    @Value("${app.kafka.topics.payment-events}")
    private String paymentEventsTopic;

    @Value("${app.kafka.topics.notification-events}")
    private String notificationEventsTopic;

    @Value("${app.kafka.topics.dead-letter}")
    private String deadLetterTopic;

    /**
     * Publishes an event to the appropriate topic based on event type.
     * Uses eventId as the partition key for ordering guarantees.
     */
    public CompletableFuture<SendResult<String, Event>> publishEvent(Event event) {
        String topic = resolveTopic(event.getEventType());
        log.info("Publishing event [{}] of type [{}] to topic [{}]",
                event.getEventId(), event.getEventType(), topic);

        return kafkaTemplate.send(topic, event.getEventId(), event)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to publish event [{}]: {}", event.getEventId(), ex.getMessage());
                    } else {
                        log.info("Event [{}] published to partition [{}] at offset [{}]",
                                event.getEventId(),
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    }
                });
    }

    /**
     * Sends a failed event to the dead-letter queue for manual inspection.
     */
    public void sendToDeadLetter(Event event, String reason) {
        log.warn("Sending event [{}] to dead-letter queue. Reason: {}", event.getEventId(), reason);
        event.setStatus(Event.EventStatus.DEAD_LETTERED);
        kafkaTemplate.send(deadLetterTopic, event.getEventId(), event);
    }

    private String resolveTopic(String eventType) {
        return switch (eventType.toUpperCase()) {
            case "ORDER_CREATED", "ORDER_UPDATED", "ORDER_CANCELLED" -> orderEventsTopic;
            case "PAYMENT_INITIATED", "PAYMENT_CONFIRMED", "PAYMENT_FAILED" -> paymentEventsTopic;
            case "NOTIFICATION_EMAIL", "NOTIFICATION_SMS" -> notificationEventsTopic;
            default -> orderEventsTopic;
        };
    }
}
