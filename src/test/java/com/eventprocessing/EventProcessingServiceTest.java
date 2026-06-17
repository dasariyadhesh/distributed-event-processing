package com.eventprocessing;

import com.eventprocessing.model.Event;
import com.eventprocessing.retry.RetryPolicy;
import com.eventprocessing.service.EventProcessingService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class EventProcessingServiceTest {

    @Mock
    private RetryPolicy retryPolicy;

    @InjectMocks
    private EventProcessingService eventProcessingService;

    private Event orderEvent;
    private Event paymentEvent;

    @BeforeEach
    void setUp() {
        orderEvent = Event.of("ORDER_CREATED", "{\"orderId\":\"ORD-001\",\"amount\":100.0}", "test");
        paymentEvent = Event.of("PAYMENT_CONFIRMED", "{\"paymentId\":\"PAY-001\"}", "test");
    }

    @Test
    void shouldProcessOrderEventSuccessfully() {
        eventProcessingService.process(orderEvent);
        assertEquals(Event.EventStatus.COMPLETED, orderEvent.getStatus());
        assertNotNull(orderEvent.getProcessedAt());
        verifyNoInteractions(retryPolicy);
    }

    @Test
    void shouldProcessPaymentEventSuccessfully() {
        eventProcessingService.process(paymentEvent);
        assertEquals(Event.EventStatus.COMPLETED, paymentEvent.getStatus());
        assertNotNull(paymentEvent.getProcessedAt());
    }

    @Test
    void shouldTriggerRetryOnFailure() {
        Event badEvent = Event.of("ORDER_CREATED", null, "test"); // null payload triggers failure
        eventProcessingService.process(badEvent);
        assertEquals(Event.EventStatus.FAILED, badEvent.getStatus());
        verify(retryPolicy, times(1)).handleFailure(eq(badEvent), any(Exception.class));
    }

    @Test
    void shouldBuildEventWithCorrectDefaults() {
        Event event = Event.of("ORDER_CREATED", "{}", "unit-test");
        assertNotNull(event.getEventId());
        assertEquals(0, event.getRetryCount());
        assertEquals(Event.EventStatus.PENDING, event.getStatus());
        assertNotNull(event.getCreatedAt());
    }
}
