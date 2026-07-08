package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/abhishekbhonde/forge/internal/job"
	"github.com/abhishekbhonde/forge/internal/queue"
)

// ═══════════════════════════════════════════════════════════════════════════════
// Fake in-memory Queue (same pattern as internal/worker tests)
// ═══════════════════════════════════════════════════════════════════════════════

type fakeQueue struct {
	mu    sync.Mutex
	store map[string]*job.Job
	pending []*job.Job
}

func newFakeQueue() *fakeQueue {
	return &fakeQueue{
		store: make(map[string]*job.Job),
	}
}

func (f *fakeQueue) Enqueue(_ context.Context, j *job.Job) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := *j
	f.store[j.ID] = &cp
	f.pending = append(f.pending, &cp)
	return nil
}

func (f *fakeQueue) Dequeue(_ context.Context, _ string) (*job.Job, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.pending) == 0 {
		return nil, errors.New("queue empty")
	}
	j := f.pending[0]
	f.pending = f.pending[1:]
	return j, nil
}

func (f *fakeQueue) Ack(_ context.Context, jobID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if j, ok := f.store[jobID]; ok {
		j.Status = job.StatusSucceeded
	}
	return nil
}

func (f *fakeQueue) Nack(_ context.Context, jobID string, errMsg string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if j, ok := f.store[jobID]; ok {
		j.LastError = errMsg
	}
	return nil
}

func (f *fakeQueue) Get(_ context.Context, jobID string) (*job.Job, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	j, ok := f.store[jobID]
	if !ok {
		return nil, errors.New("job not found")
	}
	return j, nil
}

func (f *fakeQueue) Depth(_ context.Context, _ string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return int64(len(f.pending)), nil
}

// compile-time interface check
var _ queue.Queue = (*fakeQueue)(nil)

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

func newTestServer() (*Server, *fakeQueue) {
	fq := newFakeQueue()
	s := &Server{Queue: fq}
	return s, fq
}

// ── Enqueue ──────────────────────────────────────────────────────────────────

func TestHandleEnqueue_valid(t *testing.T) {
	s, _ := newTestServer()
	handler := NewRouter(s)

	body := `{"type":"send_email","payload":{"to":"a@b.com"},"priority":5}`
	req := httptest.NewRequest(http.MethodPost, "/api/jobs", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status: got %d, want %d", rec.Code, http.StatusCreated)
	}

	var resp map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp["id"] == "" {
		t.Error("response should contain a non-empty id")
	}
	if resp["status"] != "pending" {
		t.Errorf("status: got %q, want %q", resp["status"], "pending")
	}
}

func TestHandleEnqueue_missingType(t *testing.T) {
	s, _ := newTestServer()
	handler := NewRouter(s)

	body := `{"payload":{"to":"a@b.com"}}`
	req := httptest.NewRequest(http.MethodPost, "/api/jobs", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status: got %d, want %d", rec.Code, http.StatusBadRequest)
	}

	var resp map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp["error"] == "" {
		t.Error("response should contain a non-empty error message")
	}
}

// ── Get Job ──────────────────────────────────────────────────────────────────

func TestHandleGetJob_found(t *testing.T) {
	s, fq := newTestServer()
	handler := NewRouter(s)

	// Pre-enqueue a job so we can fetch it.
	j, err := job.NewJob("test", map[string]any{"k": "v"})
	if err != nil {
		t.Fatalf("NewJob: %v", err)
	}
	if err := fq.Enqueue(context.Background(), j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/jobs/"+j.ID, nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d, want %d", rec.Code, http.StatusOK)
	}

	var got job.Job
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got.ID != j.ID {
		t.Errorf("ID: got %q, want %q", got.ID, j.ID)
	}
	if got.Type != "test" {
		t.Errorf("Type: got %q, want %q", got.Type, "test")
	}
}

func TestHandleGetJob_notFound(t *testing.T) {
	s, _ := newTestServer()
	handler := NewRouter(s)

	req := httptest.NewRequest(http.MethodGet, "/api/jobs/nonexistent-id", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status: got %d, want %d", rec.Code, http.StatusNotFound)
	}
}

// ── List Queues ──────────────────────────────────────────────────────────────

func TestHandleListQueues(t *testing.T) {
	s, _ := newTestServer()
	handler := NewRouter(s)

	req := httptest.NewRequest(http.MethodGet, "/api/queues", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d, want %d", rec.Code, http.StatusOK)
	}

	var queues []queueInfo
	if err := json.Unmarshal(rec.Body.Bytes(), &queues); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(queues) == 0 {
		t.Fatal("expected at least one queue")
	}
	if queues[0].Name != "default" {
		t.Errorf("first queue name: got %q, want %q", queues[0].Name, "default")
	}
}
