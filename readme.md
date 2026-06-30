# Forge

A distributed, Redis-backed job queue and task scheduler — built from scratch in Go.

Forge is a self-hosted alternative to tools like Sidekiq, Celery, and BullMQ. It's an end-to-end learning project covering Go, distributed systems fundamentals, and DevOps — with a core queue engine, a worker pool, a REST + gRPC API, a real-time React dashboard, and a full deployment pipeline.

> 🚧 Status: In active development. See [Roadmap](#roadmap).

---

## Why This Project

Most polished job queues are built for Node/Python/Ruby. Forge is an attempt to build a clean, Go-native one — and to use the process to properly learn goroutines, channels, `context`, graceful shutdown, and real multi-service deployment instead of following a tutorial.

## What It Does

Any application can enqueue background jobs (sending emails, processing files, generating reports — anything that shouldn't block the user) and a pool of workers processes them reliably.

- Enqueue jobs via REST or gRPC
- Configurable worker pools with adjustable concurrency
- Automatic retries with exponential backoff
- Delayed and recurring (cron-style) jobs
- Dead-letter queue with manual re-drive
- Priority queues
- Graceful shutdown (in-flight jobs finish before exit)
- Live dashboard — real-time job status, queue depth, and throughput via WebSocket
- Demo task types so anyone can submit a job and watch it move through the system live

## Architecture

```
React Dashboard ── REST + WebSocket ──> API Server (Go, REST + gRPC)
                                                 │
                                              Redis (queues, state, pub/sub)
                                                 │
                                   ┌─────────────┼─────────────┐
                              Worker 1       Worker 2       Worker N

Go SDK (gRPC client) ──> lets external apps enqueue jobs programmatically
```

**Job flow:** a job is enqueued → pushed onto a Redis queue → picked up by a worker → marked `running` → on success it's done, on failure it retries with backoff until `max_retries`, then lands in the dead-letter queue. Every state change publishes over Redis pub/sub, which the API relays to the dashboard over WebSocket.

## Tech Stack

| Layer | Choice |
|---|---|
| Core | Go |
| Queue storage | Redis |
| API | REST + gRPC |
| Dashboard | React (Next.js) |
| Containerization | Docker + docker-compose |
| CI/CD | GitHub Actions |
| Deployment | Fly.io / Railway / VPS (TBD) |

## Project Structure

```
forge/
├── cmd/
│   ├── api/          # API server entrypoint
│   └── worker/       # Worker process entrypoint
├── internal/
│   ├── queue/        # Core queue engine
│   ├── job/          # Job model and lifecycle
│   ├── worker/       # Worker pool implementation
│   ├── retry/        # Backoff strategies
│   └── storage/      # Redis client and persistence
├── api/
│   ├── rest/         # REST handlers
│   └── grpc/         # gRPC service + .proto files
├── sdk/go/           # Go client SDK
├── dashboard/        # React/Next.js frontend
├── deploy/           # Dockerfiles, docker-compose, k8s manifests
└── docs/LEARNINGS.md # Build log
```

## Getting Started

```bash
git clone https://github.com/<your-username>/forge.git
cd forge

# Start Redis, API, and workers
docker-compose up --build

# In a separate terminal, run the dashboard
cd dashboard
npm install
npm run dev
```

Dashboard: `http://localhost:3000` · API: `http://localhost:8080`

**Enqueue a job via REST:**
```bash
curl -X POST http://localhost:8080/api/jobs \
  -H "Content-Type: application/json" \
  -d '{"type": "send_email", "payload": {"to": "test@example.com"}, "max_retries": 3}'
```

**Enqueue a job via the Go SDK:**
```go
client := forge.NewClient("localhost:50051")
job, err := client.Enqueue(ctx, &forge.Job{
    Type:       "resize_image",
    Payload:    map[string]any{"url": "https://example.com/photo.jpg"},
    MaxRetries: 5,
})
```

## API Reference

| Endpoint | Method | Description |
|---|---|---|
| `/api/jobs` | POST | Enqueue a new job |
| `/api/jobs/:id` | GET | Get job status and result |
| `/api/jobs/:id/retry` | POST | Manually retry a dead-lettered job |
| `/api/queues` | GET | List queues with depth and throughput |
| `/ws` | WS | Subscribe to live job status updates |

## What I'm Learning

Tracked in [`docs/LEARNINGS.md`](docs/LEARNINGS.md): goroutines/channels in a real workload, `context.Context` for cancellation, idiomatic Go project structure, graceful shutdown, REST vs gRPC trade-offs, and running multiple deployable services in CI/CD.
