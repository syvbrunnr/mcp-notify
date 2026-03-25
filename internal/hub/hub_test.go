package hub

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

func TestGetAndClear(t *testing.T) {
	h := New(nil) // no stdin writer for tests

	// Initially empty.
	if got := h.GetAndClear(); len(got) != 0 {
		t.Errorf("expected 0 notifications, got %d", len(got))
	}

	// Add via HTTP handler.
	form := url.Values{
		"server": {"matrix"},
		"method": {"notifications/resources/updated"},
		"raw":    {`{"jsonrpc":"2.0","method":"notifications/resources/updated"}`},
	}
	req := httptest.NewRequest(http.MethodPost, "/notify", bytes.NewBufferString(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	h.HandleNotify(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	// Should have 1 notification.
	got := h.GetAndClear()
	if len(got) != 1 {
		t.Fatalf("expected 1 notification, got %d", len(got))
	}
	if got[0].Server != "matrix" {
		t.Errorf("expected server 'matrix', got %q", got[0].Server)
	}
	if got[0].Method != "notifications/resources/updated" {
		t.Errorf("expected method 'notifications/resources/updated', got %q", got[0].Method)
	}

	// After clear, should be empty.
	if got := h.GetAndClear(); len(got) != 0 {
		t.Errorf("expected 0 after clear, got %d", len(got))
	}
}

func TestNudgeOnFirstOnly(t *testing.T) {
	var buf bytes.Buffer
	h := New(&buf)

	// First notification should trigger nudge.
	addNotification(t, h, "s1", "notifications/test")
	time.Sleep(600 * time.Millisecond) // wait for async char-by-char nudge
	if buf.Len() == 0 {
		t.Error("expected nudge on first notification")
	}

	nudgeLen := buf.Len()

	// Second notification should NOT trigger another nudge.
	addNotification(t, h, "s2", "notifications/test2")
	time.Sleep(50 * time.Millisecond)
	if buf.Len() != nudgeLen {
		t.Error("expected no additional nudge on second notification")
	}

	// Clear notifications, then add another — should nudge again.
	h.GetAndClear()
	addNotification(t, h, "s3", "notifications/test3")
	time.Sleep(600 * time.Millisecond) // wait for async nudge
	if buf.Len() == nudgeLen {
		t.Error("expected nudge after clear + new notification")
	}
}

func TestStartupNotificationDoesNotBlockLater(t *testing.T) {
	var buf bytes.Buffer
	h := New(&buf)

	// Simulate startup: tools/list_changed arrives immediately.
	addNotification(t, h, "matrix", "notifications/tools/list_changed")
	time.Sleep(600 * time.Millisecond) // wait for async nudge
	if buf.Len() == 0 {
		t.Error("expected nudge even for startup notification")
	}

	// Client fetches and clears (resets nudged flag).
	h.GetAndClear()
	nudgeLen := buf.Len()

	// Later, a real message notification should nudge again.
	addNotification(t, h, "matrix", "notifications/resources/list_changed")
	time.Sleep(600 * time.Millisecond)
	if buf.Len() == nudgeLen {
		t.Error("expected nudge for real notification after clear")
	}
}

func TestExtractHint(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{"with hint", `{"jsonrpc":"2.0","method":"notifications/resources/list_changed","params":{"_hint":"dm:alice"}}`, "dm:alice"},
		{"no hint", `{"jsonrpc":"2.0","method":"notifications/resources/list_changed"}`, ""},
		{"no params", `{"jsonrpc":"2.0","method":"notifications/resources/list_changed","params":{}}`, ""},
		{"empty raw", "", ""},
		{"invalid json", "not json", ""},
		{"complex hint", `{"jsonrpc":"2.0","method":"notifications/resources/list_changed","params":{"_hint":"2x dm:alice, room:General"}}`, "2x dm:alice, room:General"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := extractHint(tt.raw); got != tt.want {
				t.Errorf("extractHint() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestSummaryMessageWithHints(t *testing.T) {
	h := New(nil)

	// Add notification with hint.
	h.mu.Lock()
	h.notifications = append(h.notifications, Notification{
		Server: "matrix-server",
		Method: "notifications/resources/list_changed",
		Raw:    `{"jsonrpc":"2.0","method":"notifications/resources/list_changed","params":{"_hint":"dm:alice"}}`,
	})
	msg := h.summaryMessage()
	h.mu.Unlock()

	expected := "[mcp-notify] 1 new notification(s) (matrix-server: 1 — dm:alice). Call the get_notifications tool to see them."
	if msg != expected {
		t.Errorf("summaryMessage() =\n  %q\nwant\n  %q", msg, expected)
	}
}

func TestSummaryMessageWithoutHints(t *testing.T) {
	h := New(nil)

	// Add notification without hint (backward compatibility).
	h.mu.Lock()
	h.notifications = append(h.notifications, Notification{
		Server: "matrix-server",
		Method: "notifications/resources/list_changed",
		Raw:    `{"jsonrpc":"2.0","method":"notifications/resources/list_changed"}`,
	})
	msg := h.summaryMessage()
	h.mu.Unlock()

	expected := "[mcp-notify] 1 new notification(s) (matrix-server: 1). Call the get_notifications tool to see them."
	if msg != expected {
		t.Errorf("summaryMessage() =\n  %q\nwant\n  %q", msg, expected)
	}
}

func TestSummaryMessageMixedServers(t *testing.T) {
	h := New(nil)

	h.mu.Lock()
	h.notifications = append(h.notifications,
		Notification{
			Server: "matrix-server",
			Method: "notifications/resources/list_changed",
			Raw:    `{"jsonrpc":"2.0","method":"notifications/resources/list_changed","params":{"_hint":"dm:alice"}}`,
		},
		Notification{
			Server: "matrix-server",
			Method: "notifications/resources/list_changed",
			Raw:    `{"jsonrpc":"2.0","method":"notifications/resources/list_changed","params":{"_hint":"room:General"}}`,
		},
		Notification{
			Server: "other-server",
			Method: "notifications/resources/list_changed",
		},
	)
	msg := h.summaryMessage()
	h.mu.Unlock()

	expected := "[mcp-notify] 3 new notification(s) (matrix-server: 2 — dm:alice, room:General, other-server: 1). Call the get_notifications tool to see them."
	if msg != expected {
		t.Errorf("summaryMessage() =\n  %q\nwant\n  %q", msg, expected)
	}
}

func addNotification(t *testing.T, h *Hub, server, method string) {
	t.Helper()
	form := url.Values{
		"server": {server},
		"method": {method},
	}
	req := httptest.NewRequest(http.MethodPost, "/notify", bytes.NewBufferString(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	h.HandleNotify(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("notify returned %d", w.Code)
	}
}

func TestIdleDetectorBackoff(t *testing.T) {
	var buf bytes.Buffer
	h := New(&buf)

	// Start idle detector with 2s base timeout (must exceed the 1s poll loop).
	h.StartIdleDetector(2*time.Second, "[test] wake up")

	// Wait for first wake-up (2s timeout + 1s poll + char injection).
	time.Sleep(4 * time.Second)
	if buf.Len() == 0 {
		t.Fatal("expected first wake-up")
	}

	// The timeout should now be 3s (2s * 1.5). idleWasSynthetic should be true.
	h.mu.Lock()
	if !h.idleWasSynthetic {
		t.Error("expected idleWasSynthetic=true after synthetic wake-up")
	}
	cur := h.idleTimeoutCur
	h.mu.Unlock()
	if cur != 3*time.Second {
		t.Errorf("expected timeout 3s after backoff, got %v", cur)
	}

	// Simulate real activity — should reset backoff.
	h.RecordActivity()
	h.mu.Lock()
	if h.idleWasSynthetic {
		t.Error("expected idleWasSynthetic=false after RecordActivity")
	}
	cur = h.idleTimeoutCur
	h.mu.Unlock()
	if cur != 2*time.Second {
		t.Errorf("expected timeout reset to 2s, got %v", cur)
	}
}

func addNotificationWithRaw(t *testing.T, h *Hub, server, method, raw string) {
	t.Helper()
	form := url.Values{
		"server": {server},
		"method": {method},
		"raw":    {raw},
	}
	req := httptest.NewRequest(http.MethodPost, "/notify", bytes.NewBufferString(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	h.HandleNotify(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("notify returned %d", w.Code)
	}
}
