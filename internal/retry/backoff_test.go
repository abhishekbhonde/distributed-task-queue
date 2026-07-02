package retry

import (
	"testing"
	"time"
)

func TestExponentialBackoff_doublesEachAttempt(t *testing.T) {
	b := ExponentialBackoff{
		Base:   1 * time.Second,
		Max:    60 * time.Second,
		Jitter: false, // disable jitter so we get exact values
	}

	cases := []struct {
		attempt  int
		expected time.Duration
	}{
		{1, 1 * time.Second},
		{2, 2 * time.Second},
		{3, 4 * time.Second},
		{4, 8 * time.Second},
		{5, 16 * time.Second},
	}

	for _, tc := range cases {
		got := b.NextDelay(tc.attempt)
		if got != tc.expected {
			t.Errorf("attempt %d: expected %v, got %v", tc.attempt, tc.expected, got)
		}
	}
}

func TestExponentialBackoff_capsAtMax(t *testing.T) {
	b := ExponentialBackoff{
		Base:   1 * time.Second,
		Max:    10 * time.Second,
		Jitter: false,
	}

	// attempt 5 would be 16s without cap — should be capped at 10s
	got := b.NextDelay(5)
	if got != 10*time.Second {
		t.Errorf("expected delay capped at 10s, got %v", got)
	}
}

func TestExponentialBackoff_jitterWithinBounds(t *testing.T) {
	b := ExponentialBackoff{
		Base:   1 * time.Second,
		Max:    60 * time.Second,
		Jitter: true,
	}

	// With jitter, attempt 3 base delay is 4s.
	// Jitter range: [0.5*4s, 1.5*4s] = [2s, 6s]
	for i := 0; i < 100; i++ {
		got := b.NextDelay(3)
		if got < 2*time.Second || got > 6*time.Second {
			t.Errorf("jittered delay out of expected range [2s, 6s]: got %v", got)
		}
	}
}

func TestExponentialBackoff_zeroAttempt(t *testing.T) {
	b := ExponentialBackoff{
		Base:   1 * time.Second,
		Max:    60 * time.Second,
		Jitter: false,
	}

	// attempt 0 or negative should behave like attempt 1
	got := b.NextDelay(0)
	if got != 1*time.Second {
		t.Errorf("expected 1s for attempt 0, got %v", got)
	}
}

func TestShouldDeadLetter(t *testing.T) {
	cases := []struct {
		attempts   int
		maxRetries int
		expected   bool
	}{
		{0, 3, false},
		{1, 3, false},
		{2, 3, false},
		{3, 3, true}, // exhausted
		{4, 3, true}, // past max (safety check)
	}

	for _, tc := range cases {
		got := ShouldDeadLetter(tc.attempts, tc.maxRetries)
		if got != tc.expected {
			t.Errorf("attempts=%d maxRetries=%d: expected %v, got %v",
				tc.attempts, tc.maxRetries, tc.expected, got)
		}
	}
}
