package hub

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"
)

// Notification is a captured MCP notification from a proxied server.
type Notification struct {
	Server    string    `json:"server"`
	Method    string    `json:"method"`
	Raw       string    `json:"raw,omitempty"`
	Timestamp time.Time `json:"timestamp"`
}

// Hub aggregates notifications from proxy instances and manages stdin injection.
type Hub struct {
	mu            sync.Mutex
	notifications []Notification
	stdinWriter  io.Writer // writes to Claude's stdin (PTY master or pipe)
	nudgeMessage string
	startedAt    time.Time // suppress nudges during startup grace period
	gracePeriod  time.Duration
}

// New creates a hub that injects nudge messages into the given writer.
// Nudges are suppressed for the first 3 seconds to let MCP servers finish their
// initial sync without flooding the client with startup notifications.
func New(stdinWriter io.Writer) *Hub {
	return &Hub{
		stdinWriter:  stdinWriter,
		nudgeMessage: "[mcp-notify] You have new notifications. Call the get_notifications tool to see them.\r",
		startedAt:    time.Now(),
		gracePeriod:  3 * time.Second,
	}
}

// HandleNotify is the HTTP handler for receiving notifications from proxies.
func (h *Hub) HandleNotify(w http.ResponseWriter, r *http.Request) {
	h.handleNotify(w, r)
}

// HandleHealth is the HTTP handler for health checks.
func (h *Hub) HandleHealth(w http.ResponseWriter, r *http.Request) {
	h.handleHealth(w, r)
}

// GetAndClear returns all pending notifications and clears the store.
func (h *Hub) GetAndClear() []Notification {
	h.mu.Lock()
	defer h.mu.Unlock()
	result := h.notifications
	h.notifications = nil
	return result
}

// Count returns the number of pending notifications.
func (h *Hub) Count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.notifications)
}

// SetWriter updates the stdin writer (used when Claude restarts with a new PTY).
func (h *Hub) SetWriter(w io.Writer) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.stdinWriter = w
}

// handleNotify receives notifications from proxy instances.
func (h *Hub) handleNotify(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}

	server := r.FormValue("server")
	method := r.FormValue("method")
	raw := r.FormValue("raw")

	if server == "" || method == "" {
		http.Error(w, "server and method required", http.StatusBadRequest)
		return
	}

	n := Notification{
		Server:    server,
		Method:    method,
		Raw:       raw,
		Timestamp: time.Now(),
	}

	h.mu.Lock()
	prevCount := len(h.notifications)
	h.notifications = append(h.notifications, n)
	h.mu.Unlock()

	log.Printf("notification from %s: %s (pending: %d)", server, method, prevCount+1)

	// Only nudge on first notification (avoid spamming stdin).
	// Skip during startup grace period to avoid flooding from initial sync.
	if prevCount == 0 && time.Since(h.startedAt) > h.gracePeriod {
		h.nudge()
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "ok")
}

// nudge writes a message to Claude's stdin to trigger tool use.
func (h *Hub) nudge() {
	if h.stdinWriter == nil {
		return
	}
	_, err := h.stdinWriter.Write([]byte(h.nudgeMessage))
	if err != nil {
		log.Printf("nudge stdin: %v", err)
	}
}

// handleHealth is a simple health check endpoint.
func (h *Hub) handleHealth(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	count := len(h.notifications)
	h.mu.Unlock()

	out, _ := json.Marshal(map[string]any{
		"status":  "ok",
		"pending": count,
	})
	w.Header().Set("Content-Type", "application/json")
	w.Write(out)
}
