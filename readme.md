# Forge

A distributed, Redis-backed job queue and task scheduler built from scratch in Go.

> ⚠️ **This is a learning project.** I'm new to Go and using it heavily at work, so I built this for hands-on practice with goroutines, channels, `context`, graceful shutdown, and distributed systems concepts. Not intended for production use.

## What It Does

Enqueue background jobs (emails, file processing, reports) and a pool of workers processes them reliably.

- REST + gRPC API for job submission
- Configurable worker pool with concurrency control
- Retries with exponential backoff
- Delayed and cron-style recurring jobs
- Dead-letter queue with manual re-drive
- Priority queues and graceful shutdown
- Live React dashboard via WebSocket

## Architecture

```
React Dashboard ── REST + WebSocket ──> API Server (Go)
                                              │
                                           Redis
                                              │
                                ┌─────────────┼─────────────┐
                           Worker 1       Worker 2       Worker N
```

## Tech Stack

Go · Redis · REST + gRPC · React (Next.js) · Docker · GitHub Actions

## Getting Started

```bash
git clone https://github.com/<your-username>/forge.git
cd forge
docker-compose up --build

# Dashboard (separate terminal)
cd dashboard && npm install && npm run dev
```

Dashboard: `http://localhost:3000` · API: `http://localhost:8080`

## API

| Endpoint | Method | Description |
|---|---|---|
| `/api/jobs` | POST | Enqueue a new job |
| `/api/jobs/:id` | GET | Get job status |
| `/api/jobs/:id/retry` | POST | Retry a dead-lettered job |
| `/api/queues` | GET | List queues |
| `/ws` | WS | Live job status updates |
