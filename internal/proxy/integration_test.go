package proxy

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestProxyForwardsNotificationsToHub tests the full proxy pipeline:
// 1. Start an HTTP hub endpoint that captures POSTed notifications
// 2. Start the proxy with a mock MCP server (echo script that writes a notification)
// 3. Verify the hub received the notification
// 4. Verify the notification was also forwarded to stdout
func TestProxyForwardsNotificationsToHub(t *testing.T) {
	// Skip if running in CI without shell access.
	if _, err := exec.LookPath("sh"); err != nil {
		t.Skip("sh not available")
	}

	// Set up a mock hub that captures notifications.
	var mu sync.Mutex
	var captured []string

	hub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/notify" && r.Method == http.MethodPost {
			r.ParseForm()
			mu.Lock()
			captured = append(captured, fmt.Sprintf("%s:%s", r.FormValue("server"), r.FormValue("method")))
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
			return
		}
		http.NotFound(w, r)
	}))
	defer hub.Close()

	// The notification the mock server will send.
	notification := `{"jsonrpc":"2.0","method":"notifications/resources/updated","params":{"uri":"messages://inbox"}}`
	// A normal response (not a notification — has an ID).
	response := `{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2024-11-05","capabilities":{},"serverInfo":{"name":"test","version":"1.0"}}}`

	// Create a mock MCP server script that outputs both messages.
	script := fmt.Sprintf(`#!/bin/sh
echo '%s'
echo '%s'
sleep 1
`, response, notification)

	// Write script to temp file.
	tmpFile, err := os.CreateTemp("", "mock-mcp-*.sh")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.WriteString(script)
	tmpFile.Close()
	os.Chmod(tmpFile.Name(), 0755)

	// Capture proxy stdout.
	origStdout := os.Stdout
	stdoutR, stdoutW, _ := os.Pipe()
	os.Stdout = stdoutW

	// Provide empty stdin — close write end immediately so io.Copy finishes.
	origStdin := os.Stdin
	stdinR, stdinW, _ := os.Pipe()
	stdinW.Close() // No input needed — close so proxy's stdin forwarding returns.
	os.Stdin = stdinR

	// Run proxy.
	p := New(hub.URL, "test-server")
	done := make(chan error, 1)
	go func() {
		done <- p.Run("sh", []string{tmpFile.Name()})
	}()

	// Wait for completion.
	select {
	case err := <-done:
		// Process exited (expected — script exits after sleep).
		_ = err
	case <-time.After(5 * time.Second):
		t.Fatal("proxy didn't exit in time")
	}

	// Restore stdout/stdin.
	stdoutW.Close()
	os.Stdout = origStdout
	os.Stdin = origStdin

	// Read captured stdout.
	var buf bytes.Buffer
	io.Copy(&buf, stdoutR)
	stdoutR.Close()
	stdinR.Close()

	output := buf.String()

	// Verify both messages were forwarded to stdout.
	if !strings.Contains(output, `"id":1`) {
		t.Error("expected response to be forwarded to stdout")
	}
	if !strings.Contains(output, `notifications/resources/updated`) {
		t.Error("expected notification to be forwarded to stdout")
	}

	// Verify hub received the notification.
	mu.Lock()
	defer mu.Unlock()

	if len(captured) != 1 {
		t.Fatalf("expected 1 notification to hub, got %d", len(captured))
	}
	if captured[0] != "test-server:notifications/resources/updated" {
		t.Errorf("unexpected captured: %s", captured[0])
	}
}
