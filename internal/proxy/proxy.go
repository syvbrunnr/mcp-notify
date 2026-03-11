package proxy

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
)

var hubClient = &http.Client{Timeout: 5 * time.Second}

// JSONRPCMessage represents a minimal JSON-RPC 2.0 message.
type JSONRPCMessage struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   json.RawMessage `json:"error,omitempty"`
}

// IsNotification returns true if this is a JSON-RPC notification (no ID field).
func (m *JSONRPCMessage) IsNotification() bool {
	return m.ID == nil && m.Method != ""
}

// Proxy is a transparent MCP stdio proxy that intercepts notifications.
type Proxy struct {
	hubURL  string
	server  string // name of the MCP server being proxied
	cmd     *exec.Cmd
	mu      sync.Mutex
	stopped bool
}

// New creates a new proxy that will forward notifications to the hub.
func New(hubURL, serverName string) *Proxy {
	return &Proxy{
		hubURL: hubURL,
		server: serverName,
	}
}

// Run starts the real MCP server as a child process and proxies stdio.
// It blocks until the child exits.
func (p *Proxy) Run(command string, args []string) error {
	p.cmd = exec.Command(command, args...)
	p.cmd.Stderr = os.Stderr

	stdin, err := p.cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("stdin pipe: %w", err)
	}
	stdout, err := p.cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("stdout pipe: %w", err)
	}

	if err := p.cmd.Start(); err != nil {
		return fmt.Errorf("start server: %w", err)
	}

	var wg sync.WaitGroup

	// Forward our stdin → child stdin (transparent).
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer stdin.Close()
		io.Copy(stdin, os.Stdin)
	}()

	// Forward child stdout → our stdout, intercepting notifications.
	wg.Add(1)
	go func() {
		defer wg.Done()
		p.forwardStdout(stdout)
	}()

	err = p.cmd.Wait()
	p.mu.Lock()
	p.stopped = true
	p.mu.Unlock()

	wg.Wait()
	return err
}

// forwardStdout reads JSON-RPC messages from the child's stdout.
// Notifications are intercepted and forwarded to the hub.
// All messages (including notifications) are written to our stdout.
func (p *Proxy) forwardStdout(r io.Reader) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 1024*1024), 10*1024*1024) // 10MB max line

	for scanner.Scan() {
		line := scanner.Bytes()

		// Always forward the line to our stdout (transparent).
		os.Stdout.Write(line)
		os.Stdout.Write([]byte("\n"))

		// Try to parse as JSON-RPC to detect notifications.
		var msg JSONRPCMessage
		if err := json.Unmarshal(line, &msg); err != nil {
			continue // Not JSON — forward as-is (already written above).
		}

		if msg.IsNotification() && strings.HasPrefix(msg.Method, "notifications/") {
			go p.sendToHub(line, msg.Method)
		}
	}

	if err := scanner.Err(); err != nil {
		p.mu.Lock()
		stopped := p.stopped
		p.mu.Unlock()
		if !stopped {
			log.Printf("scanner error: %v", err)
		}
	}
}

// sendToHub posts an intercepted notification to the hub.
func (p *Proxy) sendToHub(raw []byte, method string) {
	payload := url.Values{
		"server": {p.server},
		"method": {method},
		"raw":    {string(raw)},
	}

	resp, err := hubClient.PostForm(p.hubURL+"/notify", payload)
	if err != nil {
		log.Printf("notify hub: %v", err)
		return
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		log.Printf("hub responded %d", resp.StatusCode)
	}
}
