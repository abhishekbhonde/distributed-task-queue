// Package worker runs a pool of goroutines that pull jobs from a queue
// and execute registered handlers. This is where Forge's concurrency lives.
package worker

import (
	"context"
	"fmt"
	"sync"

	"github.com/abhishekbhonde/forge/internal/job"
	"github.com/abhishekbhonde/forge/internal/queue"
)

// HandlerFunc processes a single job. Return nil on success, an error
// to trigger retry/dead-letter via Nack.
type HandlerFunc func(ctx context.Context, j *job.Job) error

// ─── Pool ────────────────────────────────────────────────────────────────────

// Pool manages a fixed number of worker goroutines that dequeue and
// execute jobs.
type Pool struct {
	q           queue.Queue
	handlers    map[string]HandlerFunc
	concurrency int
	wg          sync.WaitGroup
	done        chan struct{}
}

// NewPool creates a worker pool that will run concurrency goroutines
// pulling from q.
func NewPool(q queue.Queue, concurrency int) *Pool {
	if concurrency < 1 {
		concurrency = 1
	}
	return &Pool{
		q:           q,
		handlers:    make(map[string]HandlerFunc),
		concurrency: concurrency,
		done:        make(chan struct{}),
	}
}

// RegisterHandler binds a HandlerFunc to a job type. When a dequeued
// job has Type == jobType, fn is called.
func (p *Pool) RegisterHandler(jobType string, fn HandlerFunc) {
	p.handlers[jobType] = fn
}

// ─── Start ───────────────────────────────────────────────────────────────────

// Start launches exactly p.concurrency goroutines that loop pulling
// jobs from queueName. It returns immediately (non-blocking).
//
// Workers stop pulling new jobs when either ctx is cancelled or
// Shutdown is called. In-flight jobs are allowed to finish.
func (p *Pool) Start(ctx context.Context, queueName string) {
	for i := 0; i < p.concurrency; i++ {
		go p.worker(ctx, queueName)
	}
}

// worker is the loop each goroutine runs.
func (p *Pool) worker(ctx context.Context, queueName string) {
	for {
		// Check stop signals before attempting to dequeue.
		select {
		case <-ctx.Done():
			return
		case <-p.done:
			return
		default:
		}

		// Dequeue blocks (polling internally) until a job arrives or
		// the context is cancelled.
		j, err := p.q.Dequeue(ctx, queueName)
		if err != nil {
			// Context cancelled means we are shutting down — exit cleanly.
			select {
			case <-ctx.Done():
				return
			case <-p.done:
				return
			default:
				// Transient dequeue error — loop and try again.
				continue
			}
		}

		// Track this in-flight job so Shutdown can wait for it.
		p.wg.Add(1)
		p.process(ctx, j)
		p.wg.Done()
	}
}

// process dispatches a single job to its registered handler and
// acks or nacks accordingly.
func (p *Pool) process(ctx context.Context, j *job.Job) {
	fn, ok := p.handlers[j.Type]
	if !ok {
		// No handler registered for this job type.
		_ = p.q.Nack(ctx, j.ID, fmt.Sprintf("no handler registered for job type %q", j.Type))
		return
	}

	if err := fn(ctx, j); err != nil {
		_ = p.q.Nack(ctx, j.ID, err.Error())
		return
	}

	_ = p.q.Ack(ctx, j.ID)
}

// ─── Shutdown ────────────────────────────────────────────────────────────────

// Shutdown signals workers to stop pulling new jobs and waits for all
// in-flight jobs to finish. Returns nil on clean shutdown, or ctx.Err()
// if the deadline passes first.
func (p *Pool) Shutdown(ctx context.Context) error {
	// Signal all workers to stop looping.
	select {
	case <-p.done:
		// Already closed.
	default:
		close(p.done)
	}

	// Wait for in-flight jobs in a separate goroutine so we can
	// respect the context deadline.
	ch := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(ch)
	}()

	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
