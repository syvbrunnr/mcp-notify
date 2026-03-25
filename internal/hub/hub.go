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
	lastNudgeAt   time.Time // when the last nudge was sent (zero if never)
	stdinWriter   io.Writer // writes to Claude's stdin (PTY master or pipe)

	// Idle detection: tracks last terminal activity (stdin or stdout).
	lastActivity    time.Time     // updated by RecordActivity()
	idleTimeout     time.Duration // BASE timeout (configured via flag)
	idleTimeoutCur  time.Duration // CURRENT timeout (increases with backoff)
	idleTimeoutMax  time.Duration // maximum backoff cap (default 120s)
	idleMessage     string        // message to inject on idle
	idleRunning     bool          // whether idle detector goroutine is active
	idleWasSynthetic bool         // true if last activity was from idle injection (not real)
}

const (
	nudgeCooldown = 15 * time.Second // minimum interval between nudges
)

// New creates a hub that injects nudge messages into the given writer.
func New(stdinWriter io.Writer) *Hub {
	return &Hub{
		stdinWriter:  stdinWriter,
		lastActivity: time.Now(),
	}
}

// RecordActivity updates the last-activity timestamp. Call this from both
// the stdin reader (user typing) and stdout tee (Claude output) to track
// any terminal activity that indicates the session is not idle.
// Also resets the exponential backoff — real activity means the session
// is productive again, so the timeout should return to base.
func (h *Hub) RecordActivity() {
	h.mu.Lock()
	h.lastActivity = time.Now()
	if h.idleWasSynthetic && h.idleTimeout > 0 {
		// Real activity after a synthetic wake-up: reset backoff
		h.idleTimeoutCur = h.idleTimeout
		h.idleWasSynthetic = false
		log.Printf("[LOG] idle detector: real activity detected, timeout reset to %v", h.idleTimeout)
	}
	h.mu.Unlock()
}

// StartIdleDetector launches a goroutine that injects a wake-up message
// when no terminal activity (stdin or stdout) has occurred for the given
// timeout. The message is clearly marked as automated so Claude doesn't
// interpret it as a user response. Safe to call multiple times — only
// the first call starts the goroutine.
func (h *Hub) StartIdleDetector(timeout time.Duration, message string) {
	h.mu.Lock()
	if h.idleRunning {
		h.mu.Unlock()
		return
	}
	h.idleRunning = true
	h.idleTimeout = timeout
	h.idleTimeoutCur = timeout
	h.idleTimeoutMax = 120 * time.Second // 2 minute cap
	h.idleMessage = message
	h.lastActivity = time.Now()
	h.mu.Unlock()

	log.Printf("[LOG] idle detector started (timeout=%v, max=%v)", timeout, 120*time.Second)

	go func() {
		for {
			time.Sleep(1 * time.Second)

			h.mu.Lock()
			idle := time.Since(h.lastActivity)
			curTimeout := h.idleTimeoutCur
			writer := h.stdinWriter
			msg := h.idleMessage
			h.mu.Unlock()

			if curTimeout == 0 || writer == nil {
				continue
			}

			if idle < curTimeout {
				continue
			}

			log.Printf("[LOG] idle detector: %v idle (timeout=%v), injecting wake-up", idle.Round(time.Second), curTimeout.Round(time.Second))

			go func() {
				full := msg + "\r"
				for _, c := range []byte(full) {
					h.mu.Lock()
					w := h.stdinWriter
					h.mu.Unlock()
					if w == nil {
						return
					}
					w.Write([]byte{c})
					time.Sleep(5 * time.Millisecond)
				}
				log.Printf("[LOG] idle detector: wake-up injected (%d bytes)", len(full))
			}()

			// Exponential backoff: increase timeout by 1.5x after each
			// synthetic wake-up. Resets when RecordActivity sees real
			// terminal output. Caps at idleTimeoutMax (120s).
			h.mu.Lock()
			h.lastActivity = time.Now()
			h.idleWasSynthetic = true
			newTimeout := time.Duration(float64(h.idleTimeoutCur) * 1.5)
			if newTimeout > h.idleTimeoutMax {
				newTimeout = h.idleTimeoutMax
			}
			if newTimeout != h.idleTimeoutCur {
				log.Printf("[LOG] idle detector: backoff %v -> %v", h.idleTimeoutCur.Round(time.Second), newTimeout.Round(time.Second))
			}
			h.idleTimeoutCur = newTimeout
			h.mu.Unlock()
		}
	}()
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
	h.lastNudgeAt = time.Time{} // reset so next notification nudges immediately
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

	// Nudge with cooldown: send at most one nudge per nudgeCooldown interval.
	// If the nudge fails (nil writer), don't update lastNudgeAt so it retries
	// on the next notification. The Stop hook serves as the final safety net
	// at response boundaries.
	h.mu.Lock()
	elapsed := time.Since(h.lastNudgeAt)
	shouldNudge := h.lastNudgeAt.IsZero() || elapsed >= nudgeCooldown
	h.mu.Unlock()

	if shouldNudge {
		if h.nudge() {
			h.mu.Lock()
			h.lastNudgeAt = time.Now()
			h.mu.Unlock()
		}
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "ok")
}

// extractHint parses the Raw JSON-RPC notification for a _hint field in params.
// Returns the hint string, or empty string if not found.
func extractHint(raw string) string {
	if raw == "" {
		return ""
	}
	var msg struct {
		Params struct {
			Hint string `json:"_hint"`
		} `json:"params"`
	}
	if err := json.Unmarshal([]byte(raw), &msg); err != nil {
		return ""
	}
	return msg.Params.Hint
}

// summaryMessage builds a notification message with source counts and optional hints.
// When notifications carry _hint metadata (e.g. "dm:alice", "room:General"),
// the summary includes them so the agent can distinguish message types.
// Must be called with h.mu held.
func (h *Hub) summaryMessage() string {
	total := len(h.notifications)

	// Per-server: count + collect hints
	type serverInfo struct {
		count int
		hints []string // non-empty hints from Raw payloads
	}
	servers := make(map[string]*serverInfo)
	for _, n := range h.notifications {
		si, ok := servers[n.Server]
		if !ok {
			si = &serverInfo{}
			servers[n.Server] = si
		}
		si.count++
		if hint := extractHint(n.Raw); hint != "" {
			si.hints = append(si.hints, hint)
		}
	}

	// Sort server names for deterministic output.
	serverNames := make([]string, 0, len(servers))
	for s := range servers {
		serverNames = append(serverNames, s)
	}
	sort.Strings(serverNames)

	var parts []string
	for _, s := range serverNames {
		si := servers[s]
		if len(si.hints) > 0 {
			// Combine all hints for this server (they may already be pre-aggregated)
			combined := strings.Join(si.hints, ", ")
			parts = append(parts, fmt.Sprintf("%s: %d — %s", s, si.count, combined))
		} else {
			parts = append(parts, fmt.Sprintf("%s: %d", s, si.count))
		}
	}

	return fmt.Sprintf("[mcp-notify] %d new notification(s) (%s). Call the get_notifications tool to see them.",
		total, strings.Join(parts, ", "))
}

// nudge delivers a notification message to Claude's stdin char-by-char.
// Char-by-char delivery (5ms between chars) avoids bracketed paste mode issues
// where the TUI can get stuck in paste-collection state, breaking subsequent
// Enter key presses. The final \r submits the notification message.
// Returns true if the nudge was initiated (writer available), false if skipped.
func (h *Hub) nudge() bool {
	h.mu.Lock()
	writer := h.stdinWriter
	h.mu.Unlock()

	if writer == nil {
		log.Printf("[LOG] nudge: SKIPPED — stdinWriter is nil")
		return false
	}
	h.mu.Lock()
	msg := h.summaryMessage()
	h.mu.Unlock()

	go func() {
		full := msg + "\r"
		written := 0
		start := time.Now()
		for _, c := range []byte(full) {
			n, err := writer.Write([]byte{c})
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
	return true
}

// HandleStopHook is the HTTP handler for Claude Code's Stop hook.
// When Claude finishes responding, this hook checks for pending notifications.
// If notifications are pending, it blocks the stop and tells Claude to fetch them.
// This is a safety net complementing PTY nudge — catches notifications that
// arrived after the last nudge cooldown or weren't acted on during tool chains.
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

	// Use bracketed paste like nudge() does, but synchronously so we can report results.
	paste := "\x1b[200~" + msg + "\x1b[201~"
	_, writeErr := writer.Write([]byte(paste))
	if writeErr == nil {
		time.Sleep(100 * time.Millisecond)
		_, writeErr = writer.Write([]byte("\r"))
	}
	full := paste + "\r"

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
