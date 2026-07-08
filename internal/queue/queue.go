// Package queue implements the core Forge engine: pushing jobs into
// Redis sorted sets by priority and pulling them out atomically.
package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/abhishekbhonde/forge/internal/job"
	"github.com/abhishekbhonde/forge/internal/retry"
	"github.com/abhishekbhonde/forge/internal/storage"
)

// ─── Interface ───────────────────────────────────────────────────────────────

// Queue defines the operations every Forge queue backend must support.
type Queue interface {
	// Enqueue stores a job and adds it to the named pending set.
	Enqueue(ctx context.Context, j *job.Job) error

	// Dequeue atomically pops the highest-priority job from queueName.
	// It blocks (polling every 500 ms) until a job is available or ctx
	// is cancelled.
	Dequeue(ctx context.Context, queueName string) (*job.Job, error)

	// Ack marks a job as succeeded.
	Ack(ctx context.Context, jobID string) error

	// Nack reports a job failure. If retries are exhausted the job moves
	// to the dead-letter list; otherwise it is re-enqueued after a
	// back-off delay.
	Nack(ctx context.Context, jobID string, errMsg string) error

	// Get retrieves a job by ID without changing its state.
	Get(ctx context.Context, jobID string) (*job.Job, error)

	// Depth returns the number of pending jobs in queueName.
	Depth(ctx context.Context, queueName string) (int64, error)
}

// ─── RedisQueue ──────────────────────────────────────────────────────────────

// RedisQueue implements Queue backed by Redis sorted sets.
type RedisQueue struct {
	rdb      *redis.Client
	OnChange func(*job.Job)
}

// NewRedisQueue creates a RedisQueue that uses the given storage.Client.
func NewRedisQueue(client *storage.Client) *RedisQueue {
	return &RedisQueue{rdb: client.Redis()}
}

// ─── Enqueue ─────────────────────────────────────────────────────────────────

// Enqueue serialises j as JSON into forge:job:<id> and adds the job ID to
// the pending sorted set with score = Priority.
func (q *RedisQueue) Enqueue(ctx context.Context, j *job.Job) error {
	data, err := json.Marshal(j)
	if err != nil {
		return fmt.Errorf("queue: marshal job: %w", err)
	}

	pipe := q.rdb.TxPipeline()

	// Store the full job state.
	pipe.Set(ctx, storage.JobKey(j.ID), data, 0)

	// Add job ID to the pending sorted set (score = priority).
	pipe.ZAdd(ctx, storage.QueuePendingKey(j.Type), redis.Z{
		Score:  float64(j.Priority),
		Member: j.ID,
	})

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("queue: enqueue pipeline: %w", err)
	}

	if q.OnChange != nil {
		q.OnChange(j)
	}
	return nil
}

// ─── Dequeue ─────────────────────────────────────────────────────────────────

// pollInterval is the delay between ZPOPMAX attempts when the queue is empty.
const pollInterval = 500 * time.Millisecond

// Dequeue atomically pops the highest-priority job ID from the pending
// sorted set, marks the job as running, and returns it.  It polls every
// 500 ms and respects ctx cancellation.
func (q *RedisQueue) Dequeue(ctx context.Context, queueName string) (*job.Job, error) {
	pendingKey := storage.QueuePendingKey(queueName)

	for {
		// Try to pop the member with the highest score.
		members, err := q.rdb.ZPopMax(ctx, pendingKey, 1).Result()
		if err != nil && err != redis.Nil {
			return nil, fmt.Errorf("queue: zpopmax: %w", err)
		}

		if len(members) == 1 {
			jobID := members[0].Member.(string)
			j, err := q.get(ctx, jobID)
			if err != nil {
				return nil, err
			}

			// Mark as running.
			j.Status = job.StatusRunning
			j.UpdatedAt = time.Now().UTC()
			if err := q.save(ctx, j); err != nil {
				return nil, err
			}

			if q.OnChange != nil {
				q.OnChange(j)
			}
			return j, nil
		}

		// Queue empty — wait and retry, honouring ctx.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(pollInterval):
			// loop again
		}
	}
}

// ─── Ack ─────────────────────────────────────────────────────────────────────

// Ack marks the job as succeeded.
func (q *RedisQueue) Ack(ctx context.Context, jobID string) error {
	j, err := q.get(ctx, jobID)
	if err != nil {
		return err
	}
	j.Status = job.StatusSucceeded
	j.UpdatedAt = time.Now().UTC()
	if err := q.save(ctx, j); err != nil {
		return err
	}

	if q.OnChange != nil {
		q.OnChange(j)
	}
	return nil
}

// ─── Nack ────────────────────────────────────────────────────────────────────

// Nack handles a failed job:
//   - Increments Attempts and records the error message.
//   - If retries exhausted → dead-letter.
//   - Otherwise → re-enqueue with a delay-based score so the job won't
//     be picked up until the back-off period expires.
func (q *RedisQueue) Nack(ctx context.Context, jobID string, errMsg string) error {
	j, err := q.get(ctx, jobID)
	if err != nil {
		return err
	}

	j.Attempts++
	j.LastError = errMsg
	j.UpdatedAt = time.Now().UTC()

	if retry.ShouldDeadLetter(j.Attempts, j.MaxRetries) {
		// Move to dead-letter.
		j.Status = job.StatusDeadLetter
		if err := q.save(ctx, j); err != nil {
			return err
		}
		if err := q.rdb.RPush(ctx, storage.QueueDeadKey(j.Type), j.ID).Err(); err != nil {
			return err
		}

		if q.OnChange != nil {
			q.OnChange(j)
		}
		return nil
	}

	// Re-enqueue with a delay score.
	j.Status = job.StatusPending
	if err := q.save(ctx, j); err != nil {
		return err
	}

	delay := retry.DefaultBackoff.NextDelay(j.Attempts)
	score := float64(time.Now().Add(delay).Unix())

	if err := q.rdb.ZAdd(ctx, storage.QueuePendingKey(j.Type), redis.Z{
		Score:  score,
		Member: j.ID,
	}).Err(); err != nil {
		return err
	}

	if q.OnChange != nil {
		q.OnChange(j)
	}
	return nil
}

// ─── Get ─────────────────────────────────────────────────────────────────────

// Get retrieves a job by ID without changing its state.
func (q *RedisQueue) Get(ctx context.Context, jobID string) (*job.Job, error) {
	return q.get(ctx, jobID)
}

// ─── Depth ───────────────────────────────────────────────────────────────────

// Depth returns ZCARD of the pending sorted set for queueName.
func (q *RedisQueue) Depth(ctx context.Context, queueName string) (int64, error) {
	n, err := q.rdb.ZCard(ctx, storage.QueuePendingKey(queueName)).Result()
	if err != nil {
		return 0, fmt.Errorf("queue: zcard: %w", err)
	}
	return n, nil
}

// ─── internal helpers ────────────────────────────────────────────────────────

// get fetches and deserialises a job from Redis.
func (q *RedisQueue) get(ctx context.Context, jobID string) (*job.Job, error) {
	data, err := q.rdb.Get(ctx, storage.JobKey(jobID)).Bytes()
	if err != nil {
		return nil, fmt.Errorf("queue: get job %s: %w", jobID, err)
	}
	var j job.Job
	if err := json.Unmarshal(data, &j); err != nil {
		return nil, fmt.Errorf("queue: unmarshal job %s: %w", jobID, err)
	}
	return &j, nil
}

// save serialises and stores a job in Redis.
func (q *RedisQueue) save(ctx context.Context, j *job.Job) error {
	data, err := json.Marshal(j)
	if err != nil {
		return fmt.Errorf("queue: marshal job: %w", err)
	}
	return q.rdb.Set(ctx, storage.JobKey(j.ID), data, 0).Err()
}

// ScanJobs scans all Redis keys matching "forge:job:*" and returns
// their deserialized job representations.
func (q *RedisQueue) ScanJobs(ctx context.Context) ([]*job.Job, error) {
	var cursor uint64
	var keys []string
	for {
		var k []string
		var err error
		k, cursor, err = q.rdb.Scan(ctx, cursor, "forge:job:*", 100).Result()
		if err != nil {
			return nil, fmt.Errorf("queue: scan jobs: %w", err)
		}
		keys = append(keys, k...)
		if cursor == 0 {
			break
		}
	}

	if len(keys) == 0 {
		return []*job.Job{}, nil
	}

	pipe := q.rdb.Pipeline()
	cmds := make([]*redis.StringCmd, len(keys))
	for i, key := range keys {
		cmds[i] = pipe.Get(ctx, key)
	}
	_, err := pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		return nil, fmt.Errorf("queue: pipeline get jobs: %w", err)
	}

	var jobs []*job.Job
	for _, cmd := range cmds {
		data, err := cmd.Bytes()
		if err != nil {
			if err == redis.Nil {
				continue
			}
			return nil, fmt.Errorf("queue: read job: %w", err)
		}
		var j job.Job
		if err := json.Unmarshal(data, &j); err != nil {
			return nil, fmt.Errorf("queue: unmarshal scanned job: %w", err)
		}
		jobs = append(jobs, &j)
	}

	return jobs, nil
}
