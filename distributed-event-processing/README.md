# Distributed Event Processing Framework

> A personal framework I built to deepen my hands-on understanding of event-driven architecture patterns after working extensively with Kafka in production at Blue Yonder and Innominds. The goal was to consolidate the patterns I had applied across real systems — retry mechanisms, idempotent consumers, DLQ routing, manual offset management — into a single well-structured, runnable reference implementation.

---

## Why I Built This

While working on enterprise systems at Blue Yonder (Yard Management) and Innominds (Commission Engine), I frequently dealt with:

- Messages being reprocessed on consumer rebalance, causing duplicate side effects
- Services failing mid-processing with no retry strategy, silently losing events
- No structured way to inspect and replay failed messages
- Tight coupling between event producers and consumers making the system brittle

I built this framework to solve those exact problems in a clean, isolated environment — and to have a reusable foundation I can adapt for future microservice projects.

---

## What This Framework Does

This is a **Spring Boot + Apache Kafka** application that demonstrates a complete event-driven processing pipeline with:

- **Async event publishing** with per-topic routing based on event type
- **Idempotent consumers** that deduplicate redelivered messages using event IDs
- **Manual offset acknowledgment** — offsets are committed only after successful processing, not on receipt
- **Exponential backoff retry** — failed events are retried up to N times with doubling delays
- **Dead-Letter Queue (DLQ)** — events that exhaust retries are routed to a separate topic for inspection
- **Concurrent listeners** — each topic runs multiple parallel consumer threads
- **Idempotent producer** — `enable.idempotence=true` with `acks=all` prevents duplicate publishes

---

## Architecture

```
                        ┌─────────────────────────────────────────┐
                        │           Kafka Broker                  │
                        │  ┌──────────────┐ ┌──────────────────┐  │
[REST API]              │  │ order-events │ │ payment-events   │  │
    │                   │  │ (3 partitions)│ │ (3 partitions)   │  │
    ▼                   │  └──────────────┘ └──────────────────┘  │
[EventProducer] ───────►│  ┌────────────────────┐ ┌───────────┐  │
    │                   │  │ notification-events │ │    DLQ    │  │
    │                   │  │   (2 partitions)    │ │(1 partition│  │
    │                   │  └────────────────────┘ └───────────┘  │
    │                   └──────────────┬──────────────────────────┘
    │                                  │
    │                            [EventConsumer]
    │                           (manual ack, idempotent)
    │                                  │
    │                        [EventProcessingService]
    │                           (type-based routing)
    │                                  │
    │                    failure?       │
    │                   ┌──────────────┘
    │                   ▼
    │              [RetryPolicy]
    │         (exponential backoff)
    │                   │
    │         retries exhausted?
    │                   ▼
    └──────────► [Dead-Letter Queue]
```

---

## Package Structure

```
com.eventprocessing
│
├── DistributedEventProcessingApplication.java   # Entry point
│
├── model/
│   └── Event.java              # Core event entity with status lifecycle
│                               # (PENDING → PROCESSING → COMPLETED / FAILED → DEAD_LETTERED)
│
├── config/
│   └── KafkaTopicConfig.java   # Declares all Kafka topics as Spring beans
│                               # Partitions and replication configured here
│
├── producer/
│   ├── EventProducer.java      # Publishes events to correct topic by event type
│   │                           # Async with CompletableFuture callbacks for success/failure logging
│   └── EventController.java    # REST endpoint to trigger event publishing (for testing)
│
├── consumer/
│   └── EventConsumer.java      # @KafkaListener per topic with manual ack
│                               # Idempotency check before processing (in-memory, Redis in prod)
│                               # Non-ack on failure forces Kafka redelivery
│
├── service/
│   └── EventProcessingService.java   # Delegates to type-specific handlers
│                                     # Catches exceptions and hands off to RetryPolicy
│
└── retry/
    └── RetryPolicy.java        # Checks retry count vs max attempts
                                # Applies exponential backoff (backoffMs * 2^retryCount)
                                # Routes to DLQ when retries exhausted
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Java 17 |
| Framework | Spring Boot 3.2 |
| Messaging | Apache Kafka + Spring Kafka |
| Build Tool | Maven |
| Boilerplate reduction | Lombok |
| Serialization | Jackson (JSON) |
| Local infra | Docker Compose |
| Testing | JUnit 5 + Mockito |

---

## How I Tested This

Testing was done at three levels:

### 1. Unit Tests (`mvn test`)
Core processing logic is unit tested with Mockito to isolate the `EventProcessingService` from Kafka:

```bash
mvn test
```

Tests cover:
- Successful processing of each event type sets status to `COMPLETED`
- Failed processing (null payload) triggers `RetryPolicy.handleFailure()`
- Event factory method `Event.of()` sets correct defaults (PENDING, retryCount=0)

### 2. Integration Testing with Embedded Kafka
For integration tests (not committed to keep the repo clean), I used `@EmbeddedKafka` from `spring-kafka-test` to spin up a real Kafka broker in-process and verify end-to-end event flow including DLQ routing.

```java
@EmbeddedKafka(partitions = 1, topics = {"order-events", "dead-letter-queue"})
@SpringBootTest
class EventFlowIntegrationTest { ... }
```

### 3. Manual Testing via REST API
Once the app is running with Kafka (see below), I tested each flow manually:

**Happy path:**
```bash
curl -X POST http://localhost:8080/api/events \
  -H "Content-Type: application/json" \
  -d '{"eventType": "ORDER_CREATED", "payload": "{\"orderId\":\"ORD-001\"}", "source": "manual-test"}'
```

**Trigger retry + DLQ (send empty payload):**
```bash
curl -X POST http://localhost:8080/api/events \
  -H "Content-Type: application/json" \
  -d '{"eventType": "PAYMENT_CONFIRMED", "payload": "", "source": "manual-test"}'
```

**Monitor via Kafka UI** at `http://localhost:8090` — watch messages move from topic → DLQ in real time.

---

## Running Locally

### Step 1 — Start Kafka
```bash
docker-compose up -d
```
This starts Zookeeper, Kafka broker, and Kafka UI (browser at `http://localhost:8090`).

### Step 2 — Build and Run
```bash
mvn clean install
mvn spring-boot:run
```

### Step 3 — Publish Events
```bash
# Order event
curl -X POST http://localhost:8080/api/events \
  -H "Content-Type: application/json" \
  -d '{"eventType": "ORDER_CREATED", "payload": "{\"orderId\":\"ORD-001\",\"amount\":250.00}", "source": "order-service"}'

# Payment event
curl -X POST http://localhost:8080/api/events \
  -H "Content-Type: application/json" \
  -d '{"eventType": "PAYMENT_CONFIRMED", "payload": "{\"paymentId\":\"PAY-001\"}", "source": "payment-service"}'

# Notification event
curl -X POST http://localhost:8080/api/events \
  -H "Content-Type: application/json" \
  -d '{"eventType": "NOTIFICATION_EMAIL", "payload": "{\"to\":\"user@example.com\"}", "source": "notification-service"}'
```

---

## Supported Event Types

| Category | Event Types | Topic |
|---|---|---|
| Order | `ORDER_CREATED`, `ORDER_UPDATED`, `ORDER_CANCELLED` | `order-events` |
| Payment | `PAYMENT_INITIATED`, `PAYMENT_CONFIRMED`, `PAYMENT_FAILED` | `payment-events` |
| Notification | `NOTIFICATION_EMAIL`, `NOTIFICATION_SMS` | `notification-events` |

---

## Configuration Reference

```yaml
app:
  kafka:
    retry:
      max-attempts: 3        # Retry up to 3 times before DLQ
      backoff-ms: 1000       # 1s → 2s → 4s exponential backoff
```

---

## Key Design Decisions

**Why manual acknowledgment?**
Kafka auto-commit ACKs the offset on receipt regardless of whether processing succeeded. Manual ack ensures the offset is only committed after the event is fully processed — preventing silent data loss on crashes.

**Why in-memory idempotency store?**
For simplicity in this framework. In production, this should be a Redis SET or a DB table with event_id as the primary key and a TTL matching your retention window.

**Why exponential backoff instead of fixed delay?**
Fixed-delay retries hammer a failing downstream service. Doubling the delay each attempt gives the dependency time to recover without flooding it.

**Why a DLQ instead of just logging failures?**
Logs are ephemeral and easy to miss. A DLQ is a durable, queryable Kafka topic — failed events can be inspected, fixed, and replayed without any code changes.

---

## Author

**Yadhesh DG**
Software Engineer — Distributed Systems & Backend Engineering

- LinkedIn: [linkedin.com/in/dasariyadhesh](https://linkedin.com/in/dasariyadhesh)
- GitHub: [github.com/dasariyadhesh](https://github.com/dasariyadhesh)
- Email: dasariyadhesh@gmail.com
