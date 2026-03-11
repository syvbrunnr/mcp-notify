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
