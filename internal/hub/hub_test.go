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
	h.startedAt = time.Now().Add(-10 * time.Second) // bypass grace period

	// First notification should trigger nudge.
	addNotification(t, h, "s1", "notifications/test")
	if buf.Len() == 0 {
		t.Error("expected nudge on first notification")
	}

	nudgeLen := buf.Len()

	// Second notification should NOT trigger another nudge.
	addNotification(t, h, "s2", "notifications/test2")
	if buf.Len() != nudgeLen {
		t.Error("expected no additional nudge on second notification")
	}

	// Clear notifications, then add another — should nudge again.
	h.GetAndClear()
	addNotification(t, h, "s3", "notifications/test3")
	if buf.Len() == nudgeLen {
		t.Error("expected nudge after clear + new notification")
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
