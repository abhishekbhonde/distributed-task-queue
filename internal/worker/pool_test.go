package worker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/abhishekbhonde/forge/internal/job"
	"github.com/abhishekbhonde/forge/internal/queue"
)

// ═══════════════════════════════════════════════════════════════════════════════
// Fake in-memory Queue — satisfies queue.Queue without Redis.
// ═══════════════════════════════════════════════════════════════════════════════

// fakeQueue is a minimal in-memory implementation of queue.Queue for unit
// testing the worker pool. No Redis needed.
type fakeQueue struct {
	mu      sync.Mutex
	jobs    []*job.Job              // pending jobs (FIFO)
	store   map[string]*job.Job     // all jobs by ID
	acked   map[string]bool         // IDs that were acked
	nacked  map[string]string       // ID → error message
}

func newFakeQueue() *fakeQueue {
	return &fakeQueue{
		store:  make(map[string]*job.Job),
		acked:  make(map[string]bool),
		nacked: make(map[string]string),
	}
}

func (f *fakeQueue) Enqueue(_ context.Context, j *job.Job) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	// Deep-copy the job so mutations don't leak.
	cp := *j
	f.store[j.ID] = &cp
	f.jobs = append(f.jobs, &cp)
	return nil
}

func (f *fakeQueue) Dequeue(ctx context.Context, _ string) (*job.Job, error) {
	// Poll every 10ms (fast for tests) until a job is available or ctx ends.
	for {
		f.mu.Lock()
		if len(f.jobs) > 0 {
			j := f.jobs[0]
			f.jobs = f.jobs[1:]
			j.Status = job.StatusRunning
			f.store[j.ID] = j
			f.mu.Unlock()
			return j, nil
		}
		f.mu.Unlock()

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (f *fakeQueue) Ack(_ context.Context, jobID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.acked[jobID] = true
	if j, ok := f.store[jobID]; ok {
		j.Status = job.StatusSucceeded
	}
	return nil
}

func (f *fakeQueue) Nack(_ context.Context, jobID string, errMsg string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nacked[jobID] = errMsg
	return nil
}

func (f *fakeQueue) Get(_ context.Context, jobID string) (*job.Job, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	j, ok := f.store[jobID]
	if !ok {
		return nil, errors.New("job not found")
	}
	return j, nil
}

func (f *fakeQueue) Depth(_ context.Context, _ string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return int64(len(f.jobs)), nil
}

// compile-time check
var _ queue.Queue = (*fakeQueue)(nil)

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

func TestPool_runsHandler(t *testing.T) {
	fq := newFakeQueue()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	j := mustNewJob(t, "email", map[string]any{"to": "a@b.com"})
	if err := fq.Enqueue(ctx, j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	called := make(chan *job.Job, 1)

	p := NewPool(fq, 1)
	p.RegisterHandler("email", func(_ context.Context, j *job.Job) error {
		called <- j
		return nil
	})
	p.Start(ctx, "default")

	select {
	case got := <-called:
		if got.ID != j.ID {
			t.Errorf("handler got job ID %q, want %q", got.ID, j.ID)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for handler to be called")
	}

	if err := p.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

func TestPool_unknownJobType(t *testing.T) {
	fq := newFakeQueue()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	j := mustNewJob(t, "unknown-type", map[string]any{"x": 1})
	if err := fq.Enqueue(ctx, j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	p := NewPool(fq, 1)
	// Deliberately NOT registering a handler for "unknown-type".
	p.Start(ctx, "default")

	// Wait until the job gets nacked.
	deadline := time.After(2 * time.Second)
	for {
		fq.mu.Lock()
		errMsg, nacked := fq.nacked[j.ID]
		fq.mu.Unlock()

		if nacked {
			if errMsg == "" {
				t.Error("nack error message should not be empty")
			}
			break
		}

		select {
		case <-deadline:
			t.Fatal("timed out waiting for job to be nacked")
		case <-time.After(10 * time.Millisecond):
		}
	}

	if err := p.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

func TestPool_shutdown(t *testing.T) {
	fq := newFakeQueue()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	p := NewPool(fq, 2)
	p.RegisterHandler("whatever", func(_ context.Context, _ *job.Job) error {
		return nil
	})
	p.Start(ctx, "default")

	// Shutdown immediately with no jobs — should return cleanly.
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer shutCancel()

	if err := p.Shutdown(shutCtx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

func TestPool_concurrency(t *testing.T) {
	fq := newFakeQueue()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const numJobs = 10

	for i := 0; i < numJobs; i++ {
		j := mustNewJob(t, "counter", map[string]any{"i": i})
		if err := fq.Enqueue(ctx, j); err != nil {
			t.Fatalf("Enqueue job %d: %v", i, err)
		}
	}

	var counter atomic.Int64
	var handlerWg sync.WaitGroup
	handlerWg.Add(numJobs)

	p := NewPool(fq, 4)
	p.RegisterHandler("counter", func(_ context.Context, _ *job.Job) error {
		counter.Add(1)
		handlerWg.Done()
		return nil
	})
	p.Start(ctx, "default")

	// Wait for all handlers to fire.
	doneCh := make(chan struct{})
	go func() {
		handlerWg.Wait()
		close(doneCh)
	}()

	select {
	case <-doneCh:
		// success
	case <-ctx.Done():
		t.Fatal("timed out waiting for all handlers to complete")
	}

	if got := counter.Load(); got != numJobs {
		t.Errorf("handler call count: got %d, want %d", got, numJobs)
	}

	if err := p.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

// ─── helpers ─────────────────────────────────────────────────────────────────

func mustNewJob(t *testing.T, jobType string, payload map[string]any, opts ...job.Option) *job.Job {
	t.Helper()
	j, err := job.NewJob(jobType, payload, opts...)
	if err != nil {
		t.Fatalf("NewJob: %v", err)
	}
	return j
}
