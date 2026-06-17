package com.eventprocessing.consumer;

import com.eventprocessing.model.Event;
import com.eventprocessing.service.EventProcessingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
@RequiredArgsConstructor
public class EventConsumer {

    private final EventProcessingService eventProcessingService;

    // Idempotency store — in production, replace with Redis or DB-backed store
    private final Set<String> processedEventIds = ConcurrentHashMap.newKeySet();

    @KafkaListener(
            topics = "${app.kafka.topics.order-events}",
            groupId = "${spring.kafka.consumer.group-id}",
            concurrency = "3"
    )
    public void consumeOrderEvent(ConsumerRecord<String, Event> record, Acknowledgment ack) {
        processEvent(record, ack);
    }

    @KafkaListener(
            topics = "${app.kafka.topics.payment-events}",
            groupId = "${spring.kafka.consumer.group-id}",
            concurrency = "3"
    )
    public void consumePaymentEvent(ConsumerRecord<String, Event> record, Acknowledgment ack) {
        processEvent(record, ack);
    }

    @KafkaListener(
            topics = "${app.kafka.topics.notification-events}",
            groupId = "${spring.kafka.consumer.group-id}",
            concurrency = "2"
    )
    public void consumeNotificationEvent(ConsumerRecord<String, Event> record, Acknowledgment ack) {
        processEvent(record, ack);
    }

    @KafkaListener(
            topics = "${app.kafka.topics.dead-letter}",
            groupId = "${spring.kafka.consumer.group-id}-dlq"
    )
    public void consumeDeadLetterEvent(ConsumerRecord<String, Event> record, Acknowledgment ack) {
        Event event = record.value();
        log.error("Dead-letter event received [{}] of type [{}] after [{}] retries. Payload: {}",
                event.getEventId(), event.getEventType(), event.getRetryCount(), event.getPayload());
        // Alert / persist for manual review
        ack.acknowledge();
    }

    /**
     * Core consumer logic with idempotency guard and manual acknowledgment.
     * Ensures each event is processed exactly once even on rebalance or redelivery.
     */
    private void processEvent(ConsumerRecord<String, Event> record, Acknowledgment ack) {
        Event event = record.value();
        String eventId = event.getEventId();

        if (processedEventIds.contains(eventId)) {
            log.warn("Duplicate event detected [{}], skipping.", eventId);
            ack.acknowledge();
            return;
        }

        try {
            eventProcessingService.process(event);
            processedEventIds.add(eventId);
            ack.acknowledge();
        } catch (Exception ex) {
            log.error("Unhandled exception for event [{}]: {}. Will not acknowledge — Kafka will redeliver.",
                    eventId, ex.getMessage());
            // Not acknowledging causes Kafka to redeliver to another consumer instance
        }
    }
}
