package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/abhishekbhonde/forge/api/rest"
	"github.com/abhishekbhonde/forge/internal/job"
	"github.com/abhishekbhonde/forge/internal/queue"
	"github.com/abhishekbhonde/forge/internal/storage"
)

// ─── Configuration ───────────────────────────────────────────────────────────

type config struct {
	Addr          string // HTTP listen address
	RedisAddr     string
	RedisPassword string
	RedisDB       int
}

func loadConfig() config {
	return config{
		Addr:          envOrDefault("ADDR", ":8080"),
		RedisAddr:     envOrDefault("REDIS_ADDR", "localhost:6379"),
		RedisPassword: envOrDefault("REDIS_PASSWORD", ""),
		RedisDB:       envOrDefaultInt("REDIS_DB", 0),
	}
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

// ─── main ────────────────────────────────────────────────────────────────────

func main() {
	cfg := loadConfig()

	// ── Redis ────────────────────────────────────────────────────────────
	log.Printf("connecting to Redis at %s (db=%d)...", cfg.RedisAddr, cfg.RedisDB)

	storageClient, err := storage.NewClient(storage.Config{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})
	if err != nil {
		log.Fatalf("redis connection failed: %v", err)
	}
	defer storageClient.Close()

	log.Println("redis connected ✓")

	// ── WebSocket Hub ────────────────────────────────────────────────────
	hub := rest.NewHub()
	hubCtx, hubCancel := context.WithCancel(context.Background())
	defer hubCancel()
	go hub.Run(hubCtx)

	// ── Queue ────────────────────────────────────────────────────────────
	q := queue.NewRedisQueue(storageClient)
	q.OnChange = func(j *job.Job) {
		msg, err := json.Marshal(map[string]any{
			"event": "job_updated",
			"job":   j,
		})
		if err == nil {
			hub.Broadcast(msg)
		}
	}

	// ── REST server ──────────────────────────────────────────────────────
	server := &rest.Server{
		Queue:  q,
		Pinger: storageClient,
		Hub:    hub,
	}
	handler := rest.NewRouter(server)

	httpServer := &http.Server{
		Addr:    cfg.Addr,
		Handler: handler,
	}

	// ── Start HTTP server ────────────────────────────────────────────────
	go func() {
		log.Printf("forge api listening on %s", cfg.Addr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server error: %v", err)
		}
	}()

	// ── Wait for shutdown signal ─────────────────────────────────────────
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	received := <-sig

	log.Printf("received %s, shutting down api...", received)

	// Give in-flight HTTP requests up to 15 seconds to finish.
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutCancel()

	if err := httpServer.Shutdown(shutCtx); err != nil {
		log.Printf("http shutdown error: %v", err)
	} else {
		log.Println("http server stopped ✓")
	}

	if err := storageClient.Close(); err != nil {
		log.Printf("redis close error: %v", err)
	} else {
		log.Println("redis connection closed ✓")
	}

	log.Println("forge api stopped cleanly ✓")
}
