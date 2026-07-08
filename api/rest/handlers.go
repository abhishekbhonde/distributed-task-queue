// Package rest provides HTTP handlers for the Forge API.
// It uses only net/http (Go 1.22+ ServeMux with path parameters).
package rest

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/abhishekbhonde/forge/internal/job"
	"github.com/abhishekbhonde/forge/internal/queue"
)

// ─── Server ──────────────────────────────────────────────────────────────────

// Pinger is implemented by storage.Client — used for health checks.
type Pinger interface {
	Ping(ctx context.Context) error
}

// Server holds the dependencies shared across all HTTP handlers.
type Server struct {
	Queue  queue.Queue
	Pinger Pinger // optional; if nil, health check always returns ok
	Hub    *Hub   // websocket hub
}

// NewRouter returns an http.Handler with all Forge REST routes registered.
func NewRouter(s *Server) http.Handler {
	mux := http.NewServeMux()

	// Health check — verifies Redis is reachable when a Pinger is set.
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		if s.Pinger != nil {
			if err := s.Pinger.Ping(r.Context()); err != nil {
				writeJSON(w, http.StatusServiceUnavailable, map[string]string{
					"status": "degraded",
					"error":  err.Error(),
				})
				return
			}
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	// WebSocket real-time subscription.
	if s.Hub != nil {
		mux.HandleFunc("GET /ws", s.wsHandler)
	}

	// Job endpoints.
	mux.HandleFunc("POST /api/jobs", s.handleEnqueue)
	mux.HandleFunc("GET /api/jobs/{id}", s.handleGetJob)
	mux.HandleFunc("POST /api/jobs/{id}/retry", s.handleRetryJob)

	// Queue endpoints.
	mux.HandleFunc("GET /api/queues", s.handleListQueues)

	return enableCORS(mux)
}

// ─── Enqueue ─────────────────────────────────────────────────────────────────

// enqueueRequest is the expected JSON body for POST /api/jobs.
type enqueueRequest struct {
	Type       string         `json:"type"`
	Payload    map[string]any `json:"payload"`
	MaxRetries *int           `json:"max_retries,omitempty"`
	Priority   *int           `json:"priority,omitempty"`
}

func (s *Server) handleEnqueue(w http.ResponseWriter, r *http.Request) {
	var req enqueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	if strings.TrimSpace(req.Type) == "" {
		writeError(w, http.StatusBadRequest, "type is required")
		return
	}

	// Build options from the request.
	var opts []job.Option
	if req.Priority != nil {
		opts = append(opts, job.WithPriority(*req.Priority))
	}
	if req.MaxRetries != nil {
		opts = append(opts, job.WithMaxRetries(*req.MaxRetries))
	}

	// Ensure payload is never nil (NewJob rejects nil).
	payload := req.Payload
	if payload == nil {
		payload = map[string]any{}
	}

	j, err := job.NewJob(req.Type, payload, opts...)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if err := s.Queue.Enqueue(r.Context(), j); err != nil {
		writeError(w, http.StatusInternalServerError, "enqueue failed: "+err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, map[string]string{
		"id":     j.ID,
		"status": string(j.Status),
	})
}

// ─── Get Job ─────────────────────────────────────────────────────────────────

func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	j, err := s.Queue.Get(r.Context(), id)
	if err != nil {
		// Treat any get error as not-found for now.
		writeError(w, http.StatusNotFound, "job not found")
		return
	}

	writeJSON(w, http.StatusOK, j)
}

// ─── Retry Job ───────────────────────────────────────────────────────────────

func (s *Server) handleRetryJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	j, err := s.Queue.Get(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}

	if j.Status != job.StatusDeadLetter {
		writeError(w, http.StatusBadRequest, "only dead_letter jobs can be retried")
		return
	}

	// Reset the job for a fresh attempt.
	j.Status = job.StatusPending
	j.Attempts = 0
	j.LastError = ""

	if err := s.Queue.Enqueue(r.Context(), j); err != nil {
		writeError(w, http.StatusInternalServerError, "re-enqueue failed: "+err.Error())
		return
	}

	writeJSON(w, http.StatusOK, j)
}

// ─── List Queues ─────────────────────────────────────────────────────────────

// queueInfo is the JSON shape returned by GET /api/queues.
type queueInfo struct {
	Name  string `json:"name"`
	Depth int64  `json:"depth"`
}

func (s *Server) handleListQueues(w http.ResponseWriter, r *http.Request) {
	names := []string{"default"}
	if lister, ok := s.Queue.(interface {
		ListQueues(ctx context.Context) ([]string, error)
	}); ok {
		if dynamicNames, err := lister.ListQueues(r.Context()); err == nil {
			names = dynamicNames
		}
	}

	result := make([]queueInfo, 0, len(names))
	for _, name := range names {
		depth, err := s.Queue.Depth(r.Context(), name)
		if err != nil {
			depth = -1 // signal error without failing the whole list
		}
		result = append(result, queueInfo{Name: name, Depth: depth})
	}

	writeJSON(w, http.StatusOK, result)
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

// writeJSON serialises v as JSON and writes it with the given status code.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// writeError responds with a JSON error body: {"error": msg}.
func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// enableCORS wraps an http.Handler with basic CORS header configuration
// and handles Preflight OPTIONS requests.
func enableCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
		w.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}
