package queue

import (
	"context"
	"testing"
	"time"

	"github.com/abhishekbhonde/forge/internal/job"
	"github.com/abhishekbhonde/forge/internal/storage"
)

// ─── helpers ─────────────────────────────────────────────────────────────────

// setup creates a storage.Client and RedisQueue, skipping the test if Redis
// is not reachable. It also flushes the test DB so every test starts clean.
func setup(t *testing.T) *RedisQueue {
	t.Helper()

	client, err := storage.NewClient(storage.Config{
		Addr: "localhost:6379",
		DB:   15, // use DB 15 for tests to avoid polluting default DB
	})
	if err != nil {
		t.Skipf("skipping: Redis not available at localhost:6379: %v", err)
	}

	// Flush the test database so tests are independent.
	client.Redis().FlushDB(context.Background())

	t.Cleanup(func() {
		client.Redis().FlushDB(context.Background())
		client.Close()
	})

	return NewRedisQueue(client)
}

// newTestJob is a shortcut for creating a job with sensible test defaults.
func newTestJob(t *testing.T, opts ...job.Option) *job.Job {
	t.Helper()
	j, err := job.NewJob("test-queue", map[string]any{"key": "value"}, opts...)
	if err != nil {
		t.Fatalf("NewJob: %v", err)
	}
	return j
}

// ─── Tests ───────────────────────────────────────────────────────────────────

func TestEnqueueAndGet(t *testing.T) {
	q := setup(t)
	ctx := context.Background()

	j := newTestJob(t, job.WithPriority(5))

	if err := q.Enqueue(ctx, j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	got, err := q.Get(ctx, j.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.ID != j.ID {
		t.Errorf("ID: got %q, want %q", got.ID, j.ID)
	}
	if got.Type != j.Type {
		t.Errorf("Type: got %q, want %q", got.Type, j.Type)
	}
	if got.Priority != 5 {
		t.Errorf("Priority: got %d, want 5", got.Priority)
	}
	if got.Status != job.StatusPending {
		t.Errorf("Status: got %q, want %q", got.Status, job.StatusPending)
	}
}

func TestDequeue(t *testing.T) {
	q := setup(t)
	ctx := context.Background()

	j := newTestJob(t)

	if err := q.Enqueue(ctx, j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// Use a short timeout so the test doesn't hang if something goes wrong.
	dctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	got, err := q.Dequeue(dctx, j.Type)
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}

	if got.ID != j.ID {
		t.Errorf("ID: got %q, want %q", got.ID, j.ID)
	}
	if got.Status != job.StatusRunning {
		t.Errorf("Status: got %q, want %q", got.Status, job.StatusRunning)
	}
}

func TestAck(t *testing.T) {
	q := setup(t)
	ctx := context.Background()

	j := newTestJob(t)

	if err := q.Enqueue(ctx, j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	dctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	if _, err := q.Dequeue(dctx, j.Type); err != nil {
		t.Fatalf("Dequeue: %v", err)
	}

	if err := q.Ack(ctx, j.ID); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	got, err := q.Get(ctx, j.ID)
	if err != nil {
		t.Fatalf("Get after Ack: %v", err)
	}
	if got.Status != job.StatusSucceeded {
		t.Errorf("Status: got %q, want %q", got.Status, job.StatusSucceeded)
	}
}

func TestNack_retry(t *testing.T) {
	q := setup(t)
	ctx := context.Background()

	j := newTestJob(t, job.WithMaxRetries(3))

	if err := q.Enqueue(ctx, j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	dctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	if _, err := q.Dequeue(dctx, j.Type); err != nil {
		t.Fatalf("Dequeue: %v", err)
	}

	if err := q.Nack(ctx, j.ID, "transient failure"); err != nil {
		t.Fatalf("Nack: %v", err)
	}

	got, err := q.Get(ctx, j.ID)
	if err != nil {
		t.Fatalf("Get after Nack: %v", err)
	}

	if got.Attempts != 1 {
		t.Errorf("Attempts: got %d, want 1", got.Attempts)
	}
	if got.Status != job.StatusPending {
		t.Errorf("Status: got %q, want %q", got.Status, job.StatusPending)
	}
	if got.LastError != "transient failure" {
		t.Errorf("LastError: got %q, want %q", got.LastError, "transient failure")
	}

	// The job should be back in the pending sorted set.
	depth, err := q.Depth(ctx, j.Type)
	if err != nil {
		t.Fatalf("Depth: %v", err)
	}
	if depth != 1 {
		t.Errorf("Depth: got %d, want 1", depth)
	}
}

func TestNack_deadLetter(t *testing.T) {
	q := setup(t)
	ctx := context.Background()

	j := newTestJob(t, job.WithMaxRetries(1))

	if err := q.Enqueue(ctx, j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	dctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	if _, err := q.Dequeue(dctx, j.Type); err != nil {
		t.Fatalf("Dequeue: %v", err)
	}

	if err := q.Nack(ctx, j.ID, "permanent failure"); err != nil {
		t.Fatalf("Nack: %v", err)
	}

	got, err := q.Get(ctx, j.ID)
	if err != nil {
		t.Fatalf("Get after Nack: %v", err)
	}

	if got.Status != job.StatusDeadLetter {
		t.Errorf("Status: got %q, want %q", got.Status, job.StatusDeadLetter)
	}

	// Job should be in the dead-letter list.
	deadLen := q.rdb.LLen(ctx, storage.QueueDeadKey(j.Type)).Val()
	if deadLen != 1 {
		t.Errorf("dead-letter list length: got %d, want 1", deadLen)
	}

	// Job should NOT be in the pending set.
	depth, err := q.Depth(ctx, j.Type)
	if err != nil {
		t.Fatalf("Depth: %v", err)
	}
	if depth != 0 {
		t.Errorf("pending Depth: got %d, want 0", depth)
	}
}

func TestDepth(t *testing.T) {
	q := setup(t)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		j := newTestJob(t, job.WithPriority(i))
		if err := q.Enqueue(ctx, j); err != nil {
			t.Fatalf("Enqueue job %d: %v", i, err)
		}
	}

	depth, err := q.Depth(ctx, "test-queue")
	if err != nil {
		t.Fatalf("Depth: %v", err)
	}
	if depth != 3 {
		t.Errorf("Depth: got %d, want 3", depth)
	}
}
