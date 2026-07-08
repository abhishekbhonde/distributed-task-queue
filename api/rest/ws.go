package rest

import (
	"context"
	"encoding/json"
	"log"
	"net/http"

	"golang.org/x/net/websocket"

	"github.com/abhishekbhonde/forge/internal/job"
)

// Hub manages active WebSocket connections.
type Hub struct {
	clients    map[chan []byte]bool
	register   chan chan []byte
	unregister chan chan []byte
	broadcast  chan []byte
}

// NewHub creates a new WebSocket Hub.
func NewHub() *Hub {
	return &Hub{
		clients:    make(map[chan []byte]bool),
		register:   make(chan chan []byte),
		unregister: make(chan chan []byte),
		broadcast:  make(chan []byte, 1024),
	}
}

// Run executes the hub registration/broadcasting loop.
func (h *Hub) Run(ctx context.Context) {
	for {
		select {
		case ch := <-h.register:
			h.clients[ch] = true
		case ch := <-h.unregister:
			if _, ok := h.clients[ch]; ok {
				delete(h.clients, ch)
				close(ch)
			}
		case msg := <-h.broadcast:
			for ch := range h.clients {
				select {
				case ch <- msg:
				default:
					// Slow consumer: skip or drop message to avoid blocking
				}
			}
		case <-ctx.Done():
			// Clean up all clients on shutdown
			for ch := range h.clients {
				close(ch)
			}
			return
		}
	}
}

// Broadcast sends a message to all registered WebSocket clients.
func (h *Hub) Broadcast(msg []byte) {
	select {
	case h.broadcast <- msg:
	default:
		log.Println("ws: broadcast buffer full, message dropped")
	}
}

// handleWebSocket upgrades HTTP connections to WebSockets, registers clients,
// streams the initial state, and keeps the connection alive.
func (s *Server) handleWebSocket(ws *websocket.Conn) {
	// Create a channel for this connection
	ch := make(chan []byte, 256)
	s.Hub.register <- ch

	// Defer unregistration and connection close
	defer func() {
		s.Hub.unregister <- ch
		_ = ws.Close()
	}()

	// Send current queue/job state immediately on connect.
	// Since s.Queue is the Queue interface, let's check if it's the concrete RedisQueue
	// or another implementation that satisfies the ScanJobs method.
	if rq, ok := s.Queue.(interface {
		ScanJobs(ctx context.Context) ([]*job.Job, error)
	}); ok {
		if jobs, err := rq.ScanJobs(context.Background()); err == nil {
			msg, err := json.Marshal(map[string]any{
				"event": "initial_state",
				"jobs":  jobs,
			})
			if err == nil {
				_, _ = ws.Write(msg)
			}
		} else {
			log.Printf("ws: error scanning jobs on connection: %v", err)
		}
	}

	// Write loop: reads from channel and writes to WebSocket
	go func() {
		for msg := range ch {
			_, err := ws.Write(msg)
			if err != nil {
				return
			}
		}
	}()

	// Read loop: keeps connection alive, detects client disconnects
	buf := make([]byte, 1024)
	for {
		_, err := ws.Read(buf)
		if err != nil {
			break
		}
	}
}

// wsHandler wraps handleWebSocket in a Server that bypasses the default CORS/origin check.
func (s *Server) wsHandler(w http.ResponseWriter, r *http.Request) {
	wsServer := websocket.Server{
		Handshake: func(config *websocket.Config, req *http.Request) error {
			// Accept any origin during handshake
			return nil
		},
		Handler: websocket.Handler(s.handleWebSocket),
	}
	wsServer.ServeHTTP(w, r)
}
