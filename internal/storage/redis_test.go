package storage

import (
	"context"
	"testing"
)

// TestNewClient_ping connects to a real Redis at localhost:6379 and pings it.
// The test is skipped (not failed) when Redis is unreachable so CI without
// Redis doesn't break.
func TestNewClient_ping(t *testing.T) {
	client, err := NewClient(Config{
		Addr: "localhost:6379",
	})
	if err != nil {
		t.Skipf("skipping: Redis not available at localhost:6379: %v", err)
	}
	defer client.Close()

	// Explicit ping through the health-check method.
	if err := client.Ping(context.Background()); err != nil {
		t.Fatalf("Ping failed on a live connection: %v", err)
	}
}

// TestKeyNames verifies that the key helper functions produce the expected
// Redis key strings.
func TestKeyNames(t *testing.T) {
	tests := []struct {
		name string
		got  string
		want string
	}{
		{
			name: "JobKey",
			got:  JobKey("abc-123"),
			want: "forge:job:abc-123",
		},
		{
			name: "JobKey with UUID",
			got:  JobKey("550e8400-e29b-41d4-a716-446655440000"),
			want: "forge:job:550e8400-e29b-41d4-a716-446655440000",
		},
		{
			name: "QueuePendingKey",
			got:  QueuePendingKey("emails"),
			want: "forge:queue:emails:pending",
		},
		{
			name: "QueuePendingKey default",
			got:  QueuePendingKey("default"),
			want: "forge:queue:default:pending",
		},
		{
			name: "QueueDeadKey",
			got:  QueueDeadKey("emails"),
			want: "forge:queue:emails:dead",
		},
		{
			name: "QueueDeadKey default",
			got:  QueueDeadKey("default"),
			want: "forge:queue:default:dead",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Errorf("got %q, want %q", tt.got, tt.want)
			}
		})
	}
}
