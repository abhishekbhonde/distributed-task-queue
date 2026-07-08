# Forge Distributed Task Queue Architecture

This document describes the high-level system architecture, data flow, component design, and underlying Redis data structures that power the Forge task queue.

---

## 1. System Architecture Diagram

```mermaid
flowchart TB
    %% Clients & UI
    subgraph UI ["User Interfaces & Clients"]
        Dashboard["Next.js 14 Dashboard\n(:3000)"]
        CurlClient["HTTP API Clients\n(curl / SDKs)"]
    end

    %% API Layer
    subgraph API_Layer ["API Layer (State Handler)"]
        APIServer["Go API Server\n(:8080)"]
        WSHub["WebSocket Hub\n(Real-time Streams)"]
        MuxRouter["http.ServeMux\n(REST Handlers)"]
    end

    %% Storage & Queue Layer
    subgraph DataStore ["State & Queue Store (Redis)"]
        RedisDB[("Redis Datastore\n(:6379)")]
        JobHash["Job State\n(JSON Strings)\nforge:job:<id>"]
        PendingSet["Sorted Sets (Priority)\nforge:queue:<name>:pending"]
        DeadList["Dead-letter Lists\nforge:queue:<name>:dead"]
    end

    %% Worker Layer
    subgraph WorkerPools ["Worker Pools (Concurrency Processing)"]
        WorkerEmail["Worker Email Pool\n(QUEUE_NAME=send_email)"]
        WorkerImage["Worker Image Pool\n(QUEUE_NAME=resize_image)"]
        WorkerReport["Worker Report Pool\n(QUEUE_NAME=generate_report)"]
    end

    %% Connections
    Dashboard -- HTTP API --> APIServer
    Dashboard -- WebSockets --> WSHub
    CurlClient -- HTTP API --> MuxRouter
    APIServer --> MuxRouter
    MuxRouter --> WSHub

    %% API to Redis
    APIServer -- Enqueue / Get / Retry --> RedisDB
    RedisDB -.-> JobHash
    RedisDB -.-> PendingSet
    RedisDB -.-> DeadList

    %% Workers to Redis
    WorkerEmail -- Dequeue / Ack / Nack --> RedisDB
    WorkerImage -- Dequeue / Ack / Nack --> RedisDB
    WorkerReport -- Dequeue / Ack / Nack --> RedisDB

    %% State change broadcasts
    RedisDB -. State Change Hook .-> APIServer
    WSHub -- Broadcast Event --> Dashboard
```

---

## 2. Component Directory

### A. Next.js 14 Dashboard
- **Frontend App**: Built with TypeScript, React 18, and styled with glassmorphism CSS.
- **WebSocket Listener**: Subscribes to the Go API WebSocket gateway to stream job transitions (pending $\rightarrow$ running $\rightarrow$ completed) instantly.
- **Polling Loop**: Periodically requests active queue counts from `/api/queues` every 5 seconds to update the stats header.

### B. Go API Server
- **REST Engine**: Exposes `/api/jobs` for enqueuing new work, `/api/jobs/{id}` for querying, `/api/jobs/{id}/retry` for manual dead-letter release, and `/api/queues` for list views.
- **WebSocket Hub**: Standardizes connection register/unregister streams and coordinates non-blocking JSON event broadcasts (`job_updated`) to clients.
- **Graceful Shutdown**: Blocks on SIGINT/SIGTERM, giving HTTP requests 15 seconds to flush before terminating the server.

### C. Redis Queue Backend
- **Data Layer**: Translates logical queue calls into atomic Redis pipelines.
- **Queue Lister**: Scans for active queue namespaces in Redis to allow dynamic UI cards without configuration.

### D. Worker Pools
- **Goroutine Pools**: Coordinates worker routines executing concurrency slots using a worker channel + `sync.WaitGroup` framework.
- **Clean Interrupts**: Guarantees that in-flight jobs run to completion on shutdown (up to 30 seconds) before letting the process exit.

---

## 3. Redis Data Structures

Forge stores all parameters and tracks job schedules inside Redis using three key structures:

| Key Format | Type | Description |
| :--- | :--- | :--- |
| **`forge:job:<id>`** | `String` (JSON) | Stores the full job parameters (ID, Payload, MaxRetries, Attempts, Status, LastError, Timestamps). |
| **`forge:queue:<name>:pending`** | `Sorted Set (ZSET)` | Queue buffer where `Score = Priority` (highest priority popped first). If a job is back-off delayed, `Score = Unix Timestamp` when it becomes runable. |
| **`forge:queue:<name>:dead`** | `List (LPUSH)` | Stores the IDs of permanently failed jobs that require manual operator retry. |

---

## 4. Job Lifecycle & Technical Flow

```mermaid
sequenceDiagram
    autonumber
    actor User as User/Browser
    participant API as API Server
    participant DB as Redis Datastore
    participant WP as Worker Pool

    User->>API: POST /api/jobs (Job Parameters)
    API->>DB: Write forge:job:<id> (Status=pending)
    API->>DB: ZADD forge:queue:<name>:pending (Score=Priority)
    API-->>User: Return 201 Created (Job ID)
    API->>User: WS Broadcast: job_updated (pending)

    Note over WP,DB: Worker Dequeue Loop
    WP->>DB: ZPOPMAX forge:queue:<name>:pending (Atomically Pop ID)
    DB-->>WP: Returns Job ID
    WP->>DB: Update forge:job:<id> (Status=running)
    API->>User: WS Broadcast: job_updated (running)
    WP->>WP: Execute Handler

    alt Execution Successful
        WP->>DB: Update forge:job:<id> (Status=succeeded)
        API->>User: WS Broadcast: job_updated (succeeded)
    else Execution Fails & Max Retries Not Exhausted
        WP->>DB: Increment attempts & update last error
        WP->>DB: ZADD forge:queue:<name>:pending (Score = UnixTime + Backoff)
        API->>User: WS Broadcast: job_updated (pending)
    else Execution Fails & Max Retries Exhausted
        WP->>DB: Update forge:job:<id> (Status=dead_letter)
        WP->>DB: RPUSH forge:queue:<name>:dead (Job ID)
        API->>User: WS Broadcast: job_updated (dead_letter)
    end
```
