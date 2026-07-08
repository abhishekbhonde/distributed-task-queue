// Package storage provides Redis-backed persistence for Forge.
// It wraps go-redis and defines the key naming scheme used by
// every other component (queue, worker, scheduler).
package storage

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// ─── Configuration ───────────────────────────────────────────────────────────

// Config holds the parameters needed to connect to a Redis instance.
type Config struct {
	Addr     string // host:port, e.g. "localhost:6379"
	Password string // "" means no password
	DB       int    // 0–15, default 0
}

// ─── Client ──────────────────────────────────────────────────────────────────

// Client wraps a go-redis client and provides Forge-specific helpers.
type Client struct {
	rdb *redis.Client
}

// NewClient connects to Redis using cfg and verifies the connection with PING.
// Returns an error if Redis is unreachable.
func NewClient(cfg Config) (*Client, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	// Verify the connection is alive before handing the client back.
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		// Clean up the underlying connection pool on failure.
		_ = rdb.Close()
		return nil, fmt.Errorf("redis ping: %w", err)
	}

	return &Client{rdb: rdb}, nil
}

// Ping sends a PING to Redis and returns any error.
// Useful as a health-check endpoint.
func (c *Client) Ping(ctx context.Context) error {
	return c.rdb.Ping(ctx).Err()
}

// Close releases the underlying Redis connection pool.
func (c *Client) Close() error {
	return c.rdb.Close()
}

// Redis returns the underlying go-redis client for use by other packages
// (queue, worker) that need to execute Redis commands directly.
func (c *Client) Redis() *redis.Client {
	return c.rdb
}

// ─── Key naming scheme ───────────────────────────────────────────────────────
//
// All Forge keys live under the "forge:" prefix so they are easy to
// identify and isolate in a shared Redis instance.
//
//   forge:job:<id>              → Hash/String holding the full job state (JSON)
//   forge:queue:<name>:pending  → Sorted set of job IDs scored by priority
//   forge:queue:<name>:dead     → List of dead-lettered job IDs

const keyPrefix = "forge"

// JobKey returns the Redis key for a job's full state.
//
//	forge:job:abc-123
func JobKey(id string) string {
	return fmt.Sprintf("%s:job:%s", keyPrefix, id)
}

// QueuePendingKey returns the sorted-set key for a queue's pending jobs.
//
//	forge:queue:emails:pending
func QueuePendingKey(queueName string) string {
	return fmt.Sprintf("%s:queue:%s:pending", keyPrefix, queueName)
}

// QueueDeadKey returns the list key for a queue's dead-lettered jobs.
//
//	forge:queue:emails:dead
func QueueDeadKey(queueName string) string {
	return fmt.Sprintf("%s:queue:%s:dead", keyPrefix, queueName)
}
