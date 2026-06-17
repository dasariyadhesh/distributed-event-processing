package com.eventprocessing.producer;

import com.eventprocessing.model.Event;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/events")
@RequiredArgsConstructor
public class EventController {

    private final EventProducer eventProducer;

    /**
     * Publishes a new event to the appropriate Kafka topic.
     *
     * Example request:
     * POST /api/events
     * {
     *   "eventType": "ORDER_CREATED",
     *   "payload": "{\"orderId\":\"ORD-001\",\"amount\":250.00}",
     *   "source": "order-service"
     * }
     */
    @PostMapping
    public ResponseEntity<Map<String, String>> publishEvent(@RequestBody Map<String, String> request) {
        String eventType = request.get("eventType");
        String payload = request.get("payload");
        String source = request.getOrDefault("source", "api");

        Event event = Event.of(eventType, payload, source);
        eventProducer.publishEvent(event);

        return ResponseEntity.accepted().body(Map.of(
                "eventId", event.getEventId(),
                "status", "PUBLISHED",
                "eventType", eventType
        ));
    }

    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> health() {
        return ResponseEntity.ok(Map.of("status", "UP", "service", "distributed-event-processing"));
    }
}
