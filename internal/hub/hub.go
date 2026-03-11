package hub

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"strings"
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
	nudged        bool      // true if a nudge has been sent for current batch
	stdinWriter   io.Writer // writes to Claude's stdin (PTY master or pipe)
}

// New creates a hub that injects nudge messages into the given writer.
func New(stdinWriter io.Writer) *Hub {
	return &Hub{
		stdinWriter: stdinWriter,
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
	h.nudged = false
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

	log.Printf("[LOG] notification from %s: %s (pending: %d)", server, method, prevCount+1)

	// Nudge once per batch. Reset by GetAndClear when client fetches notifications.
	h.mu.Lock()
	alreadyNudged := h.nudged
	if !alreadyNudged {
		h.nudged = true
	}
	h.mu.Unlock()

	if !alreadyNudged {
		h.nudge()
		// Retry nudge after 3 minutes if notifications are still pending
		go func() {
			time.Sleep(3 * time.Minute)
			if h.Count() > 0 {
				log.Printf("[LOG] retry nudge: %d notifications still pending after 3m", h.Count())
				h.nudge()
			}
		}()
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "ok")
}

// summaryMessage builds a notification message with source counts.
// Must be called with h.mu held.
func (h *Hub) summaryMessage() string {
	counts := make(map[string]int)
	for _, n := range h.notifications {
		counts[n.Server]++
	}
	total := len(h.notifications)

	// Sort server names for deterministic output.
	servers := make([]string, 0, len(counts))
	for s := range counts {
		servers = append(servers, s)
	}
	sort.Strings(servers)

	var parts []string
	for _, s := range servers {
		parts = append(parts, fmt.Sprintf("%s: %d", s, counts[s]))
	}

	return fmt.Sprintf("[mcp-notify] %d new notification(s) (%s). Call the get_notifications tool to see them.",
		total, strings.Join(parts, ", "))
}

// nudge simulates typing a message into Claude's stdin character by character.
// Bulk writes to the PTY master are ignored by the TUI, but character-by-character
// delivery (like real keyboard input) is accepted even during processing.
func (h *Hub) nudge() {
	if h.stdinWriter == nil {
		log.Printf("[LOG] nudge: SKIPPED — stdinWriter is nil")
		return
	}
	h.mu.Lock()
	msg := h.summaryMessage()
	h.mu.Unlock()
	log.Printf("[LOG] nudge: starting char-by-char injection (%d bytes + CR)", len(msg))
	go func() {
		full := msg + "\r"
		written := 0
		start := time.Now()
		for _, c := range []byte(full) {
			n, err := h.stdinWriter.Write([]byte{c})
			if err != nil {
				log.Printf("[LOG] nudge: WRITE ERROR after %d/%d bytes: %v", written, len(full), err)
				return
			}
			written += n
			time.Sleep(5 * time.Millisecond)
		}
		elapsed := time.Since(start)
		log.Printf("[LOG] nudge: COMPLETE — wrote %d bytes in %v", written, elapsed)
	}()
}

// HandleStopHook is the HTTP handler for Claude Code's Stop hook.
// When Claude finishes responding, this hook checks for pending notifications.
// If notifications are pending, it blocks the stop and tells Claude to fetch them.
// This replaces PTY stdin injection which is unreliable when the TUI is busy.
func (h *Hub) HandleStopHook(w http.ResponseWriter, r *http.Request) {
	h.handleStopHook(w, r)
}

func (h *Hub) handleStopHook(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}

	h.mu.Lock()
	count := len(h.notifications)
	var msg string
	if count > 0 {
		msg = h.summaryMessage()
	}
	h.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")

	if count > 0 {
		log.Printf("[LOG] stop hook: blocking — %d pending notifications", count)
		out, _ := json.Marshal(map[string]any{
			"decision": "block",
			"reason":   msg,
		})
		w.Write(out)
		return
	}

	out, _ := json.Marshal(map[string]any{
		"decision": "approve",
	})
	w.Write(out)
}

// HandleTestNudge triggers a nudge for diagnostic purposes.
// Call via: curl -X POST localhost:9781/test-nudge
// Use this to test nudge delivery while Claude is idle vs generating.
func (h *Hub) HandleTestNudge(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}

	msg := r.FormValue("msg")
	if msg == "" {
		msg = "[mcp-notify] test nudge — if you see this as a user message, PTY delivery works"
	}

	log.Printf("[LOG] test-nudge: injecting %d bytes", len(msg))
	h.mu.Lock()
	writer := h.stdinWriter
	h.mu.Unlock()

	if writer == nil {
		http.Error(w, "no stdin writer available", http.StatusServiceUnavailable)
		return
	}

	// Write char-by-char like nudge() does, but synchronously so we can report results.
	full := msg + "\r"
	var writeErr error
	for _, c := range []byte(full) {
		_, err := writer.Write([]byte{c})
		if err != nil {
			writeErr = err
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	w.Header().Set("Content-Type", "application/json")
	if writeErr != nil {
		log.Printf("[LOG] test-nudge: write error: %v", writeErr)
		out, _ := json.Marshal(map[string]any{
			"status": "error",
			"error":  writeErr.Error(),
			"bytes":  len(full),
		})
		w.Write(out)
		return
	}

	log.Printf("[LOG] test-nudge: wrote %d bytes successfully", len(full))
	out, _ := json.Marshal(map[string]any{
		"status": "ok",
		"bytes":  len(full),
		"msg":    msg,
	})
	w.Write(out)
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
