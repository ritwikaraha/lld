# Design and Implement a Thread-Safe Event Notification System Supporting Short Polling and Long Polling

## 1. Problem Statement

Design and implement an event notification system in Java that allows producers to publish events to topics and allows subscribed clients to retrieve events using short polling or long polling.

The system must support concurrent publishers and consumers, efficient event retrieval, thread-safe subscription management, timeout handling, and cursor-based event consumption.

The initial implementation should work within a single JVM. The design should also explain how the system could evolve into a distributed, highly available event delivery platform.

## 2. Functional Requirements

1. **Publish events:** Producers can publish events to a named topic.
2. **Store events:** Persist events in an in-memory, bounded event store with unique, monotonically increasing event IDs.
3. **Subscribe:** Clients can subscribe to specific topics and unsubscribe when they no longer need notifications.
4. **Short polling:** A client periodically requests events after its last-seen cursor. The server immediately returns available events or an empty response.
5. **Long polling:** A client sends a request and waits until events become available or a configured timeout expires.
6. **Cursor-based consumption:** Clients can resume retrieving events from their last-seen cursor.
7. **Concurrent access:** Multiple producers and consumers can publish, subscribe, unsubscribe, and poll concurrently.
8. **Resource cleanup:** Expired waiters and inactive subscriptions must be cleaned up.
9. **Event retention:** The system must define how it handles events that have been evicted from bounded storage.
10. **Batch retrieval:** Clients can specify a maximum number of events per poll.

## 3. Non-Functional Requirements

- Thread-safe event storage and subscription management.
- No lost wake-ups when events arrive during long-poll registration.
- Bounded memory consumption.
- Avoid holding a global lock while a request waits for an event.
- Correct handling of timeouts, cancellation, and spurious wake-ups.
- Efficient lookup of waiters by topic.
- Clearly defined delivery guarantees and cursor semantics.

## 4. Scope and Assumptions

The first implementation is an in-memory, single-JVM system.

- Events are retained in a bounded circular buffer.
- A topic identifies a logical event stream.
- A subscription determines which topics a client is authorized and registered to consume.
- The cursor identifies the last event processed by a client.
- Long polling is implemented using Java concurrency primitives rather than continuously spinning.
- Event storage is not durable across process restarts.
- Exactly-once processing is not guaranteed by the polling mechanism itself.

For the initial implementation, a global event ID sequence is used. A production implementation may instead use per-topic offsets or a durable log's partition offsets.

## 5. High-Level Architecture

The design separates event storage, subscription management, polling, and waiter notification.

### Components

**PollingController**

- Entry point for publish, subscribe, unsubscribe, short-poll, and long-poll operations.
- Validates requests and delegates to the appropriate services.

**EventStore**

- Stores published events in a bounded circular buffer.
- Assigns monotonically increasing IDs.
- Retrieves events after a supplied cursor.
- Detects when a requested cursor has fallen behind retained history.

**SubscriptionService**

- Maintains subscriber-to-topic and topic-to-subscriber relationships.
- Supports subscription creation, removal, and validation.
- Prevents clients from polling topics to which they are not subscribed.

**ShortPollHandler**

- Queries the event store immediately.
- Returns matching events without waiting.

**LongPollHandler**

- Returns immediately if matching events already exist.
- Otherwise registers a waiter and waits for notification or timeout.

**LongPollBroker**

- Coordinates event publication and long-poll waiter registration.
- Ensures that the check-for-events and register-waiter sequence cannot race with publication.

**WaitHandle**

- Represents one pending long-poll request.
- Uses `ReentrantLock` and `Condition` to block and wake the request thread safely.

**PollRequest / PollResponse**

- Encapsulate request parameters and response metadata, including the event batch, cursor, and timeout status.

## 6. Class Responsibilities

| Class | Responsibility |
|---|---|
| `Event` | Immutable event ID, topic, payload, and timestamp |
| `Subscription` | Subscriber ID, topic, and subscription status |
| `SubscriptionService` | Manages subscriptions and validates polling permissions |
| `EventStore` | Stores events, retrieves by cursor, and handles retention |
| `PollRequest` | Topic, subscriber ID, cursor, batch size, and timeout |
| `PollResponse` | Returned events, next cursor, and timeout or resynchronization status |
| `ShortPollHandler` | Immediate event retrieval |
| `LongPollHandler` | Blocking event retrieval with timeout |
| `LongPollBroker` | Coordinates event publication and waiter registration |
| `WaitHandle` | Encapsulates a waiting request's signal and timeout state |
| `PollingController` | Exposes the use cases through a unified interface |

## 7. Data Structures

### Event storage

Use a bounded circular buffer:

`Event[] buffer`

Maintain:

- `head`: index of the oldest retained event.
- `size`: number of retained events.
- `capacity`: maximum number of events.
- `nextEventId`: ID assigned to the next published event.

When the buffer is full, publishing a new event evicts the oldest retained event.

Advantages:

- Constant-time append.
- Predictable memory usage.
- No unbounded growth.

Limitation:

- A cursor older than retained history cannot be served reliably.
- A simple array-based implementation may scan events to filter by topic and cursor.

For higher throughput, use a per-topic log or indexed data structure rather than scanning the entire buffer.

### Subscription registry

Maintain two indexes:

- `subscriberId -> set of topics`
- `topic -> set of subscriber IDs`

This supports efficient subscription validation and removal.

For a production system, subscriptions may also include authorization, expiry, consumer groups, delivery filters, and durable subscription offsets.

### Long-poll waiter registry

Maintain:

`topic -> list of WaitHandle`

When an event is published to a topic, only waiters registered for that topic need to be considered.

This avoids scanning all outstanding long-poll requests.

## 8. API Design

### Publish an event

`publish(topic, payload) -> Event`

Creates an event, stores it, and signals interested long-poll requests.

### Subscribe

`subscribe(subscriberId, topic) -> Subscription`

Creates a subscription if one does not already exist.

### Unsubscribe

`unsubscribe(subscriberId, topic) -> boolean`

Removes the subscription and prevents future poll requests from being accepted for that subscription.

### Short poll

`shortPoll(subscriberId, topic, cursor, maxEvents) -> PollResponse`

Returns available events immediately. If no events are available, returns an empty batch.

### Long poll

`longPoll(subscriberId, topic, cursor, maxEvents, timeout) -> PollResponse`

Returns immediately when events exist; otherwise waits for an event or timeout.

### Example request

`longPoll("consumer-1", "orders", 105, 100, 30 seconds)`

The consumer requests up to 100 events after cursor 105 and waits for at most 30 seconds.

## 9. Short-Poll Flow

1. Validate the request and subscription.
2. Read events after the supplied cursor.
3. Filter by topic and batch size.
4. Return the matching events immediately.
5. If no events are available, return an empty response.
6. The client waits for its polling interval and issues another request.

Short polling is simple, but frequent requests can generate unnecessary network traffic and server load.

## 10. Long-Poll Flow

1. Validate the subscription.
2. Acquire the broker coordination lock.
3. Check the event store for events after the supplied cursor.
4. If events exist, return them immediately.
5. Otherwise, register a `WaitHandle` for the requested topic while still holding the coordination lock.
6. Release the broker lock.
7. Wait on the handle's condition variable until signalled or timed out.
8. Remove the waiter registration safely.
9. Query the event store again and return any matching events.
10. If no events are available, return a timeout response.

The second event-store query is important: the notification only indicates that something may have changed. The event store remains the source of truth.

## 11. Critical Concurrency Scenarios

### A. Preventing lost wake-ups

Consider this unsafe sequence:

1. Consumer checks the event store and finds no events.
2. Producer publishes an event and signals current waiters.
3. Consumer registers its waiter.
4. Consumer waits indefinitely despite the event already existing.

The fix is to make the **check-for-events and waiter-registration sequence atomic relative to publication**.

Use a shared broker coordination lock:

- The consumer acquires the lock, checks the event store, and registers its waiter before releasing the lock.
- The producer acquires the same lock, stores the event, and removes the relevant waiters from the registry.
- The producer releases the broker lock before signalling waiters.

This ensures that a publication cannot slip between the consumer's check and registration.

A `ConcurrentHashMap` alone does not solve this compound-operation race.

### B. Avoid blocking while holding the global lock

Never wait on a condition while holding the broker lock.

Otherwise, the producer may be unable to acquire that lock to publish the event and wake the consumer.

The correct sequence is:

1. Register the waiter under the broker lock.
2. Release the broker lock.
3. Wait on the individual `WaitHandle`.
4. Clean up the registration after completion.

### C. Signal outside the broker lock

The producer should collect and remove the relevant waiters while holding the broker lock, then signal them after releasing it.

This minimizes lock contention and avoids holding a global lock while waking multiple request threads.

### D. Spurious wake-ups

A condition variable may wake without a corresponding event notification.

Therefore, waiting must use a loop that checks the completion condition and remaining timeout rather than assuming every wake-up represents an event.

### E. Publish concurrent events

Concurrent publishers must receive unique event IDs, and event insertion must be thread-safe.

The event ID assignment and append operation must follow a consistent ordering. Assigning IDs independently from insertion can otherwise cause cursor-based consumers to miss events if IDs become visible out of order.

### F. Unsubscribe while a request is waiting

Unsubscription must define what happens to an outstanding long poll.

A reasonable policy is:

- Mark or remove the subscription.
- Cancel associated waiters where feasible.
- Reject future polls for that subscription.
- Revalidate subscription state before returning a response.

The implementation must synchronize this policy with event publication and waiter cleanup.

### G. Timeout and cancellation

Waiters must be removed whether they finish due to an event, timeout, interruption, cancellation, or unsubscription.

Use `try/finally` for cleanup and restore the thread's interrupt status when converting an interruption into an application-level response.

## 12. Event Delivery Semantics

The event store is the source of truth; notifications are only wake-up signals.

A client should advance its cursor only after it has successfully processed the corresponding events. If it advances before processing and then fails, it may skip events.

The practical guarantee is typically **at-least-once processing when clients retry from a persisted cursor**, provided events remain retained and cursor advancement follows successful processing.

Consequently:

- Consumers should process events idempotently.
- The client should persist its last successfully processed cursor.
- A response should include the cursor associated with the last event returned.
- A timeout should not advance the cursor.
- If the cursor is older than the retained history, the server should return an explicit `CURSOR_EXPIRED` or `RESYNC_REQUIRED` outcome.

A bounded in-memory store alone cannot guarantee delivery across process restarts.

## 13. Complexity Analysis

For the straightforward bounded-array implementation:

| Operation | Complexity |
|---|---|
| Append to circular buffer | O(1) |
| Scan for events after a cursor | O(N) |
| Register a waiter | O(1) average for topic lookup, excluding list-management costs |
| Signal waiters for a topic | O(W) for W registered waiters |
| Add a subscription | O(1) average with hash-based indexes |
| Remove a subscription | O(1) average with hash-based indexes, excluding cleanup of associated entries |

Here, N is the number of retained events and W is the number of waiters for the affected topic.

A production event store can use per-topic offsets and indexed logs to avoid repeatedly scanning unrelated events.

## 14. Production Considerations

### Multiple application instances

The in-memory broker works only within one JVM. If a consumer is connected to instance A and an event is published through instance B, instance A will not automatically receive that notification.

A distributed design needs:

- A shared durable event log or event store.
- A shared notification mechanism, such as Kafka or Redis Pub/Sub.
- Stable cursor or offset semantics.
- Consumer ownership and subscription coordination.
- Backpressure, connection limits, and timeout limits.
- Authentication, authorization, and tenant isolation.
- Metrics for active waiters, timeout rates, lag, event retention, and publish latency.

A notification broker may wake the consumer, but consumers should still retrieve authoritative events from the durable store.

### Scalability

- Partition events by topic or tenant.
- Use per-topic logs and offsets.
- Apply bounded queues and request limits.
- Limit the number of concurrent long-poll requests.
- Consider asynchronous or non-blocking request handling at high connection counts.
- Use durable storage if event loss across restarts is unacceptable.

## 15. Interview Discussion Points

Be prepared to explain:

1. Why use long polling instead of repeatedly short polling?
2. How does the system prevent lost wake-ups?
3. Why is `ConcurrentHashMap` insufficient for the full check-register-publish protocol?
4. Why must the producer signal waiters outside the broker lock?
5. How do you handle spurious wake-ups and timeouts?
6. What happens if the requested cursor has expired?
7. How do you guarantee unique event IDs under concurrent publication?
8. How should unsubscribe interact with in-flight long polls?
9. How would you replace the in-memory store with a durable event log?
10. How would you scale the design across multiple JVMs?
11. Does long polling guarantee exactly-once delivery?
12. How would you prevent a slow consumer from exhausting server threads or memory?

## 16. Recommended Implementation Order

1. Implement the immutable `Event` and `PollResponse` models.
2. Implement the bounded, thread-safe `EventStore`.
3. Implement subscription registration and validation.
4. Implement the short-poll handler.
5. Implement `WaitHandle` using `ReentrantLock` and `Condition`.
6. Implement the `LongPollBroker` with atomic check-and-register coordination.
7. Implement the long-poll handler and timeout cleanup.
8. Add concurrent publishing and polling tests.
9. Add race-condition tests for publication during waiter registration.
10. Add cursor-expiration, interruption, unsubscribe, and retention tests.

The final design should demonstrate clean separation of responsibilities, correct synchronization, bounded resource usage, and explicit delivery semantics—not merely two polling methods.
