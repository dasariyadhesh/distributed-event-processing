package com.eventprocessing.retry;

import com.eventprocessing.model.Event;
import com.eventprocessing.producer.EventProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class RetryPolicy {

    private final EventProducer eventProducer;

    @Value("${app.kafka.retry.max-attempts}")
    private int maxAttempts;

    @Value("${app.kafka.retry.backoff-ms}")
    private long backoffMs;

    /**
     * Evaluates whether an event should be retried or dead-lettered.
     * Applies exponential backoff between retry attempts.
     */
    public boolean shouldRetry(Event event) {
        return event.getRetryCount() < maxAttempts;
    }

    /**
     * Retries event processing with exponential backoff.
     * If max attempts are exhausted, routes to dead-letter queue.
     */
    public void handleFailure(Event event, Exception ex) {
        int currentRetry = event.getRetryCount();

        if (shouldRetry(event)) {
            long delay = backoffMs * (long) Math.pow(2, currentRetry);
            log.warn("Retrying event [{}], attempt [{}/{}] after {}ms delay. Error: {}",
                    event.getEventId(), currentRetry + 1, maxAttempts, delay, ex.getMessage());

            try {
                Thread.sleep(delay);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }

            event.setRetryCount(currentRetry + 1);
            event.setStatus(Event.EventStatus.PENDING);
            eventProducer.publishEvent(event);

        } else {
            log.error("Max retry attempts [{}] exhausted for event [{}]. Routing to DLQ.",
                    maxAttempts, event.getEventId());
            eventProducer.sendToDeadLetter(event, "Max retries exhausted: " + ex.getMessage());
        }
    }
}
