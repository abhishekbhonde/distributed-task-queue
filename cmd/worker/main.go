package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/abhishekbhonde/forge/internal/job"
	"github.com/abhishekbhonde/forge/internal/queue"
	"github.com/abhishekbhonde/forge/internal/storage"
	"github.com/abhishekbhonde/forge/internal/worker"
)

// ─── Configuration ───────────────────────────────────────────────────────────

type config struct {
	RedisAddr     string
	RedisPassword string
	RedisDB       int
	Concurrency   int
	QueueName     string
}

// loadConfig reads configuration from environment variables with sensible
// defaults so the worker runs out-of-the-box against a local Redis.
func loadConfig() config {
	cfg := config{
		RedisAddr:     envOrDefault("REDIS_ADDR", "localhost:6379"),
		RedisPassword: envOrDefault("REDIS_PASSWORD", ""),
		RedisDB:       envOrDefaultInt("REDIS_DB", 0),
		Concurrency:   envOrDefaultInt("CONCURRENCY", 5),
		QueueName:     envOrDefault("QUEUE_NAME", "default"),
	}
	return cfg
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envOrDefaultInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		log.Printf("warning: invalid %s=%q, using default %d", key, v, fallback)
		return fallback
	}
	return n
}

// ─── Demo handlers ───────────────────────────────────────────────────────────
// These simulate real work — they log the job details, sleep, and succeed.

func handleSendEmail(_ context.Context, j *job.Job) error {
	log.Printf("[send_email]    job=%s payload=%v", j.ID, j.Payload)
	time.Sleep(500 * time.Millisecond)
	return nil
}

func handleResizeImage(_ context.Context, j *job.Job) error {
	log.Printf("[resize_image]  job=%s payload=%v", j.ID, j.Payload)
	time.Sleep(1 * time.Second)
	return nil
}

func handleGenerateReport(_ context.Context, j *job.Job) error {
	log.Printf("[gen_report]    job=%s payload=%v", j.ID, j.Payload)
	time.Sleep(2 * time.Second)
	return nil
}

// ─── main ────────────────────────────────────────────────────────────────────

func main() {
	cfg := loadConfig()

	// ── Redis ────────────────────────────────────────────────────────────
	log.Printf("connecting to Redis at %s (db=%d)...", cfg.RedisAddr, cfg.RedisDB)

	client, err := storage.NewClient(storage.Config{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})
	if err != nil {
		log.Fatalf("redis connection failed: %v", err)
	}
	defer client.Close()

	log.Println("redis connected ✓")

	// ── Queue ────────────────────────────────────────────────────────────
	q := queue.NewRedisQueue(client)

	// ── Worker pool ──────────────────────────────────────────────────────
	pool := worker.NewPool(q, cfg.Concurrency)

	pool.RegisterHandler("send_email", handleSendEmail)
	pool.RegisterHandler("resize_image", handleResizeImage)
	pool.RegisterHandler("generate_report", handleGenerateReport)

	// Create a cancellable context for all workers.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool.Start(ctx, cfg.QueueName)

	log.Printf("forge worker started — queue=%q concurrency=%d",
		cfg.QueueName, cfg.Concurrency)
	log.Printf("registered handlers: send_email, resize_image, generate_report")
	log.Println("press Ctrl+C to stop")

	// ── Wait for shutdown signal ─────────────────────────────────────────
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	received := <-sig

	log.Printf("received %s, shutting down worker...", received)

	// Cancel the worker context so Dequeue loops unblock.
	cancel()

	// Give in-flight jobs up to 30 seconds to finish.
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutCancel()

	if err := pool.Shutdown(shutCtx); err != nil {
		log.Printf("shutdown timed out: %v", err)
	} else {
		log.Println("worker stopped cleanly ✓")
	}
}
