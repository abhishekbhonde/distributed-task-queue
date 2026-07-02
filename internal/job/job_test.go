package job

import (
	"testing"
)

func TestNewJob_valid(t *testing.T) {
	j, err := NewJob("send_email", map[string]any{"to": "test@example.com"})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if j.ID == "" {
		t.Error("expected a non-empty ID")
	}
	if j.Type != "send_email" {
		t.Errorf("expected type 'send_email', got %q", j.Type)
	}
	if j.Status != StatusPending {
		t.Errorf("expected status 'pending', got %q", j.Status)
	}
	if j.MaxRetries != 3 {
		t.Errorf("expected default MaxRetries=3, got %d", j.MaxRetries)
	}
	if j.Attempts != 0 {
		t.Errorf("expected Attempts=0, got %d", j.Attempts)
	}
	if j.CreatedAt.IsZero() {
		t.Error("expected CreatedAt to be set")
	}
}

func TestNewJob_withOptions(t *testing.T) {
	j, err := NewJob(
		"resize_image",
		map[string]any{"url": "https://example.com/photo.jpg"},
		WithPriority(5),
		WithMaxRetries(10),
	)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if j.Priority != 5 {
		t.Errorf("expected priority 5, got %d", j.Priority)
	}
	if j.MaxRetries != 10 {
		t.Errorf("expected MaxRetries=10, got %d", j.MaxRetries)
	}
}

func TestNewJob_emptyType(t *testing.T) {
	_, err := NewJob("", map[string]any{"key": "value"})
	if err == nil {
		t.Error("expected error for empty job type, got nil")
	}
}

func TestNewJob_nilPayload(t *testing.T) {
	_, err := NewJob("send_email", nil)
	if err == nil {
		t.Error("expected error for nil payload, got nil")
	}
}

func TestNewJob_negativeMaxRetries(t *testing.T) {
	_, err := NewJob("send_email", map[string]any{}, WithMaxRetries(-1))
	if err == nil {
		t.Error("expected error for negative MaxRetries, got nil")
	}
}

func TestNewJob_uniqueIDs(t *testing.T) {
	j1, _ := NewJob("type_a", map[string]any{})
	j2, _ := NewJob("type_a", map[string]any{})
	if j1.ID == j2.ID {
		t.Error("expected unique IDs, got the same one twice")
	}
}
