// Package retry implements backoff strategies for failed jobs.
// Pure math — no Redis, no Job dependency. Independently testable.
package retry

import (
	"math"
	"math/rand"
	"time"
)

// Strategy computes the delay before the next retry attempt.
type Strategy interface {
	NextDelay(attempt int) time.Duration
}

// ExponentialBackoff doubles the delay on each attempt, up to a max.
// Jitter spreads retries so multiple failed jobs don't all retry at once.
//
// Example with Base=1s, Max=30s:
//
//	attempt 1 → ~1s
//	attempt 2 → ~2s
//	attempt 3 → ~4s
//	attempt 4 → ~8s
//	attempt 5 → ~16s
//	attempt 6 → ~30s (capped)
type ExponentialBackoff struct {
	Base   time.Duration
	Max    time.Duration
	Jitter bool
}

// DefaultBackoff is what Forge uses unless overridden per job type.
var DefaultBackoff = ExponentialBackoff{
	Base:   1 * time.Second,
	Max:    30 * time.Second,
	Jitter: true,
}

// NextDelay returns how long to wait before the next retry.
// attempt is 1-indexed: pass 1 after the first failure, 2 after the second, etc.
func (e ExponentialBackoff) NextDelay(attempt int) time.Duration {
	if attempt <= 0 {
		attempt = 1
	}

	// delay = Base * 2^(attempt-1)
	delay := float64(e.Base) * math.Pow(2, float64(attempt-1))

	// cap at Max
	if delay > float64(e.Max) {
		delay = float64(e.Max)
	}

	if e.Jitter {
		// randomize within [0.5*delay, 1.5*delay] to spread retries out
		delay = delay*0.5 + rand.Float64()*delay
	}

	return time.Duration(delay)
}

// ShouldDeadLetter reports whether a job has exhausted its retries.
// Call this after incrementing Attempts — if true, move to dead-letter
// instead of scheduling another retry.
func ShouldDeadLetter(attempts, maxRetries int) bool {
	return attempts >= maxRetries
}
