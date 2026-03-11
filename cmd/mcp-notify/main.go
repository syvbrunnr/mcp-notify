// mcp-notify wraps Claude Code with a transparent MCP notification proxy.
// It intercepts MCP server notifications, buffers them, and injects nudge
// messages into Claude's PTY stdin so that Claude can be alerted to new
// notifications without polling. Also supports token rotation and session
// resume on restart.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/creack/pty"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"github.com/Vegard-/mcp-notify/internal/hub"
	"golang.org/x/term"
)

const (
	defaultPort = 9781
	tokenFile   = ".claude-active-token" // relative to DATA_DIR

	stdinBufSize          = 4096
	restartMsgDelay       = 5 * time.Second
	restartFlushDelay     = 10 * time.Second
	restartSignalInterval = 5 * time.Second
	shutdownTimeout       = 5 * time.Second
	stopTimeout           = 10 * time.Second
)

// processManager handles the Claude Code child process lifecycle,
// including restart with session resume and token rotation.
type processManager struct {
	mu             sync.Mutex
	cmd            *exec.Cmd
	ptmx           *os.File    // PTY master — for stdin injection
	stdinMu        *sync.Mutex // protects ptmx writes
	mcpConfig      string      // path to rewritten MCP config
	baseArgs       []string    // args passed to claude
	extraArgs      []string    // args added by restart (--resume etc.)
	exited         chan struct{}
	restarting     bool
	restartMessage string
	hub            *hub.Hub
	skipPerms      bool // pass --dangerously-skip-permissions
	stdinStarted   bool // ensures stdin reader goroutine starts only once
}

func newProcessManager(mcpConfig string, baseArgs []string, h *hub.Hub, skipPerms bool) *processManager {
	return &processManager{
		mcpConfig: mcpConfig,
		baseArgs:  baseArgs,
		stdinMu:   &sync.Mutex{},
		exited:    make(chan struct{}),
		hub:       h,
		skipPerms: skipPerms,
	}
}

// startClaude spawns a new Claude Code process with PTY stdin.
func (pm *processManager) startClaude() error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	// Build args.
	var args []string
	if pm.skipPerms {
		args = append(args, "--dangerously-skip-permissions")
	}
	if pm.mcpConfig != "" {
		args = append(args, "--mcp-config", pm.mcpConfig)
	}
	args = append(args, pm.extraArgs...)
	args = append(args, pm.baseArgs...)

	// Consume restart message.
	msg := pm.restartMessage
	pm.restartMessage = ""

	log.Printf("starting claude with args: %v", args)

	cmd := exec.Command("claude", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	// Token rotation: read active token if available.
	token := readActiveToken()
	if token != "" {
		cmd.Env = buildCleanEnv(token)
		log.Printf("token source: %s", tokenSource())
	}

	// Always use PTY — needed for notification nudge injection.
	ptmx, pts, err := pty.Open()
	if err != nil {
		return fmt.Errorf("open pty: %w", err)
	}

	// Propagate outer terminal size to inner PTY so Claude's TUI renders correctly.
	// Without this, the inner PTY defaults to 80x24 which breaks Claude's layout,
	// especially in Docker where the PTY chain is longer.
	if term.IsTerminal(int(os.Stdin.Fd())) {
		if sz, err := pty.GetsizeFull(os.Stdin); err == nil {
			_ = pty.Setsize(ptmx, sz)
		}
	}

	cmd.Stdin = pts

	if err := cmd.Start(); err != nil {
		pts.Close()
		ptmx.Close()
		return fmt.Errorf("start claude: %w", err)
	}
	pts.Close() // only child needs slave

	pm.cmd = cmd
	pm.ptmx = ptmx
	pm.exited = make(chan struct{})

	// Update hub's stdin writer to new PTY.
	safeWriter := &syncWriter{w: ptmx, mu: pm.stdinMu}
	pm.hub.SetWriter(safeWriter)

	// Inject restart message after delay if needed.
	if msg != "" {
		go func() {
			time.Sleep(restartMsgDelay)
			log.Printf("injecting restart message (%d bytes)", len(msg))
			pm.stdinMu.Lock()
			ptmx.Write([]byte(msg + "\r"))
			pm.stdinMu.Unlock()
		}()
	}

	// Start a single persistent stdin reader goroutine (only once).
	// On restart, only the PTY target (pm.ptmx) changes — the reader
	// always writes to the current PTY via pm.stdinMu + pm.ptmx.
	// This avoids the double-read race where old and new goroutines
	// both call os.Stdin.Read() simultaneously, losing bytes.
	if !pm.stdinStarted {
		pm.stdinStarted = true
		go func() {
			buf := make([]byte, stdinBufSize)
			for {
				n, err := os.Stdin.Read(buf)
				if n > 0 {
					pm.stdinMu.Lock()
					if pm.ptmx != nil {
						pm.ptmx.Write(buf[:n])
					}
					pm.stdinMu.Unlock()
				}
				if err != nil {
					return
				}
			}
		}()
	}

	// Monitor process exit.
	go func() {
		err := cmd.Wait()
		log.Printf("claude exited (pid %d, err: %v)", cmd.Process.Pid, err)
		close(pm.exited)
	}()

	log.Printf("claude started (pid %d)", cmd.Process.Pid)
	return nil
}

// stopClaude gracefully stops the current Claude process.
func (pm *processManager) stopClaude() error {
	pm.mu.Lock()
	cmd := pm.cmd
	ptmx := pm.ptmx
	exited := pm.exited
	pm.mu.Unlock()

	if cmd == nil || cmd.Process == nil {
		return nil
	}

	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		return nil
	}

	select {
	case <-exited:
		log.Println("claude stopped gracefully")
	case <-time.After(stopTimeout):
		log.Println("claude didn't stop in time, sending SIGKILL")
		_ = cmd.Process.Kill()
		<-exited
	}

	if ptmx != nil {
		ptmx.Close()
	}
	return nil
}

// --- Token rotation ---

func dataDir() string {
	if dir := os.Getenv("DATA_DIR"); dir != "" {
		return dir
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ".mcp-data/r7-tools"
	}
	return filepath.Join(home, ".mcp-data", "r7-tools")
}

func readActiveToken() string {
	path := filepath.Join(dataDir(), tokenFile)
	data, err := os.ReadFile(path)
	if err == nil {
		token := strings.TrimSpace(string(data))
		if token != "" {
			return token
		}
	}
	return os.Getenv("CLAUDE_CODE_OAUTH_TOKEN")
}

func tokenSource() string {
	path := filepath.Join(dataDir(), tokenFile)
	if _, err := os.Stat(path); err == nil {
		return "file:" + path
	}
	return "env:CLAUDE_CODE_OAUTH_TOKEN"
}

func buildCleanEnv(token string) []string {
	env := os.Environ()
	result := make([]string, 0, len(env)+1)
	for _, e := range env {
		if !strings.HasPrefix(e, "CLAUDE_CODE_OAUTH_TOKEN=") {
			result = append(result, e)
		}
	}
	return append(result, "CLAUDE_CODE_OAUTH_TOKEN="+token)
}

// --- Session detection ---

func detectSessionID() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	projectsDir := filepath.Join(home, ".claude", "projects")
	matches, err := filepath.Glob(filepath.Join(projectsDir, "*", "*.jsonl"))
	if err != nil || len(matches) == 0 {
		return ""
	}
	sort.Slice(matches, func(i, j int) bool {
		fi, _ := os.Stat(matches[i])
		fj, _ := os.Stat(matches[j])
		if fi == nil || fj == nil {
			return false
		}
		return fi.ModTime().After(fj.ModTime())
	})
	return strings.TrimSuffix(filepath.Base(matches[0]), ".jsonl")
}

// syncWriter synchronizes writes to the PTY master fd.
// Multiple goroutines (stdin reader, hub nudge, restart message injection)
// may write concurrently; this prevents interleaved or corrupted output.
type syncWriter struct {
	w  io.Writer
	mu *sync.Mutex
}

func (sw *syncWriter) Write(p []byte) (int, error) {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	return sw.w.Write(p)
}

// --- Config rewriting ---

func rewriteConfig(srcPath, proxyBin string, hubPort int) (string, error) {
	data, err := os.ReadFile(srcPath)
	if err != nil {
		return "", fmt.Errorf("read config: %w", err)
	}

	var config map[string]any
	if err := json.Unmarshal(data, &config); err != nil {
		return "", fmt.Errorf("parse config: %w", err)
	}

	servers, _ := config["mcpServers"].(map[string]any)
	if servers == nil {
		servers = make(map[string]any)
	}

	hubURL := fmt.Sprintf("http://localhost:%d", hubPort)

	for name, v := range servers {
		srv, ok := v.(map[string]any)
		if !ok {
			continue
		}
		srvType, _ := srv["type"].(string)
		if srvType != "" && srvType != "stdio" {
			continue
		}
		origCmd, _ := srv["command"].(string)
		if origCmd == "" {
			continue
		}
		origArgs, _ := srv["args"].([]any)
		newArgs := []any{"--hub", hubURL, "--name", name, "--", origCmd}
		for _, a := range origArgs {
			newArgs = append(newArgs, a)
		}
		srv["command"] = proxyBin
		srv["args"] = newArgs
		servers[name] = srv
	}

	servers["mcp-notify"] = map[string]any{
		"type": "http",
		"url":  hubURL + "/mcp",
	}
	config["mcpServers"] = servers

	out, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return "", err
	}

	dstPath := filepath.Join(os.TempDir(), fmt.Sprintf("mcp-notify-config-%d.json", os.Getpid()))
	if err := os.WriteFile(dstPath, out, 0644); err != nil {
		return "", err
	}
	return dstPath, nil
}

// --- Proxy binary discovery ---

func findProxyBinary(explicit string) string {
	if explicit != "" {
		if _, err := os.Stat(explicit); err == nil {
			return explicit
		}
		return ""
	}
	self, err := os.Executable()
	if err == nil {
		candidate := filepath.Join(filepath.Dir(self), "mcp-notify-proxy")
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
	}
	if p, err := exec.LookPath("mcp-notify-proxy"); err == nil {
		return p
	}
	return ""
}

// --- MCP tool registration ---

func registerTools(mcpServer *server.MCPServer, h *hub.Hub, pm *processManager, port int) {
	// get_notifications — returns and clears buffered MCP notifications.
	notifyTool := mcp.NewTool(
		"get_notifications",
		mcp.WithDescription("Get pending MCP notifications from proxied servers. Returns and clears all buffered notifications."),
	)
	mcpServer.AddTool(notifyTool, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		notifications := h.GetAndClear()
		if len(notifications) == 0 {
			return mcp.NewToolResultText("No pending notifications."), nil
		}
		out, err := json.MarshalIndent(notifications, "", "  ")
		if err != nil {
			return mcp.NewToolResultText(fmt.Sprintf("error marshaling notifications: %v", err)), nil
		}
		return mcp.NewToolResultText(string(out)), nil
	})

	// restart_claude — restart with session resume and token rotation.
	restartTool := mcp.NewTool(
		"restart_claude",
		mcp.WithDescription("Restart Claude Code. Picks up new MCP configs, credential changes, or recovers from issues. Resumes the most recent session by default."),
		mcp.WithBoolean("continue", mcp.Description("Resume the most recent session (default: true)")),
		mcp.WithString("resume_id", mcp.Description("Resume a specific session by ID")),
		mcp.WithString("message", mcp.Description("Message to inject as initial prompt after restart")),
	)
	mcpServer.AddTool(restartTool, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		log.Println("restart_claude called")

		shouldContinue := req.GetBool("continue", true)
		resumeID := req.GetString("resume_id", "")
		message := req.GetString("message", "")

		var newArgs []string
		if resumeID != "" {
			newArgs = append(newArgs, "--resume", resumeID)
		} else if shouldContinue {
			if sid := detectSessionID(); sid != "" {
				log.Printf("auto-detected session: %s", sid)
				newArgs = append(newArgs, "--resume", sid)
			} else {
				newArgs = append(newArgs, "--continue")
			}
		}

		if message == "" {
			message = "You have been restarted by mcp-notify. Continue your previous work."
		}

		pm.mu.Lock()
		pm.extraArgs = newArgs
		pm.restartMessage = message
		pm.mu.Unlock()

		go func() {
			pm.mu.Lock()
			pm.restarting = true
			pm.mu.Unlock()

			log.Printf("restart: waiting %s for tool result flush...", restartFlushDelay)
			time.Sleep(restartFlushDelay)

			// SIGINT → wait → SIGTERM → wait → force kill + start new.
			pm.mu.Lock()
			cmd := pm.cmd
			pm.mu.Unlock()
			if cmd != nil && cmd.Process != nil {
				log.Println("restart: sending SIGINT...")
				_ = cmd.Process.Signal(syscall.SIGINT)
			}

			log.Printf("restart: waiting %s for SIGINT shutdown...", restartSignalInterval)
			time.Sleep(restartSignalInterval)

			pm.mu.Lock()
			cmd = pm.cmd
			pm.mu.Unlock()
			if cmd != nil && cmd.Process != nil {
				log.Println("restart: sending SIGTERM...")
				_ = cmd.Process.Signal(syscall.SIGTERM)
			}

			log.Printf("restart: waiting %s for graceful shutdown...", restartSignalInterval)
			time.Sleep(restartSignalInterval)

			log.Println("restart: stopping old process...")
			_ = pm.stopClaude()

			log.Println("restart: starting new process...")
			if err := pm.startClaude(); err != nil {
				log.Printf("restart: start failed: %v", err)
			}

			pm.mu.Lock()
			pm.restarting = false
			pm.mu.Unlock()
			log.Println("restart: complete")
		}()

		return mcp.NewToolResultText("Claude Code will restart in ~20 seconds (10s flush + SIGINT 5s + SIGTERM 5s). Session will be resumed automatically."), nil
	})

	// wrapper_status — show process state, token source, port.
	statusTool := mcp.NewTool(
		"wrapper_status",
		mcp.WithDescription("Show mcp-notify wrapper status: active token source, Claude process state, notification count."),
	)
	mcpServer.AddTool(statusTool, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		pm.mu.Lock()
		cmd := pm.cmd
		pm.mu.Unlock()

		pid := 0
		if cmd != nil && cmd.Process != nil {
			pid = cmd.Process.Pid
		}

		result := map[string]any{
			"token_source":        tokenSource(),
			"claude_pid":          pid,
			"mcp_port":            port,
			"pending_notifications": h.Count(),
		}
		out, err := json.MarshalIndent(result, "", "  ")
		if err != nil {
			return mcp.NewToolResultText(fmt.Sprintf("error marshaling status: %v", err)), nil
		}
		return mcp.NewToolResultText(string(out)), nil
	})
}

// --- Main ---

func main() {
	log.SetOutput(os.Stderr)
	log.SetPrefix("[mcp-notify] ")

	mcpConfig := flag.String("mcp-config", "", "Path to MCP config JSON")
	port := flag.Int("port", defaultPort, "Hub port")
	proxyBin := flag.String("proxy-bin", "", "Path to mcp-notify-proxy binary")
	skipPerms := flag.Bool("skip-permissions", false, "Pass --dangerously-skip-permissions to Claude")
	flag.Parse()

	claudeArgs := flag.Args()

	// Find proxy binary.
	proxyPath := findProxyBinary(*proxyBin)
	if proxyPath == "" {
		log.Fatal("cannot find mcp-notify-proxy binary — specify with --proxy-bin or ensure it's in PATH or same directory")
	}
	log.Printf("proxy binary: %s", proxyPath)

	// Rewrite MCP config to insert proxy wrappers.
	var rewrittenConfig string
	if *mcpConfig != "" {
		var err error
		rewrittenConfig, err = rewriteConfig(*mcpConfig, proxyPath, *port)
		if err != nil {
			log.Fatalf("rewrite config: %v", err)
		}
		log.Printf("rewritten config: %s", rewrittenConfig)
	}

	// Create hub (writer set later when Claude starts).
	h := hub.New(nil)

	// Create process manager.
	pm := newProcessManager(rewrittenConfig, claudeArgs, h, *skipPerms)

	// Build MCP server with all tools.
	mcpServer := server.NewMCPServer("mcp-notify", "1.0.0", server.WithToolCapabilities(true))
	registerTools(mcpServer, h, pm, *port)

	// Start HTTP server.
	httpMux := http.NewServeMux()
	httpMux.HandleFunc("/notify", h.HandleNotify)
	httpMux.HandleFunc("/health", h.HandleHealth)
	httpMux.HandleFunc("/hook/stop", h.HandleStopHook)
	mcpHTTP := server.NewStreamableHTTPServer(mcpServer)
	httpMux.Handle("/mcp", mcpHTTP)

	httpServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", *port),
		Handler: httpMux,
	}

	ln, err := net.Listen("tcp", httpServer.Addr)
	if err != nil {
		log.Fatalf("listen %s: %v", httpServer.Addr, err)
	}
	log.Printf("hub listening on %s", httpServer.Addr)
	go httpServer.Serve(ln)

	// Set the real terminal to raw mode so keystrokes pass through
	// transparently to Claude's PTY. Without this, the outer terminal
	// does its own line buffering and Enter key translation, which
	// breaks Claude's TUI (especially in Docker where there's a double
	// PTY layer).
	if term.IsTerminal(int(os.Stdin.Fd())) {
		oldState, err := term.MakeRaw(int(os.Stdin.Fd()))
		if err != nil {
			log.Printf("warning: failed to set raw mode on stdin: %v", err)
		} else {
			defer term.Restore(int(os.Stdin.Fd()), oldState)
		}
	}

	// Start Claude.
	if err := pm.startClaude(); err != nil {
		log.Fatalf("initial start: %v", err)
	}

	// Main loop: wait for exit or signal, support restart.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	// Propagate terminal resize (SIGWINCH) from outer terminal to inner PTY.
	winchCh := make(chan os.Signal, 1)
	signal.Notify(winchCh, syscall.SIGWINCH)
	go func() {
		for range winchCh {
			pm.mu.Lock()
			ptmx := pm.ptmx
			pm.mu.Unlock()
			if ptmx != nil && term.IsTerminal(int(os.Stdin.Fd())) {
				if sz, err := pty.GetsizeFull(os.Stdin); err == nil {
					_ = pty.Setsize(ptmx, sz)
				}
			}
		}
	}()

	for {
		pm.mu.Lock()
		exited := pm.exited
		pm.mu.Unlock()

		select {
		case <-exited:
			pm.mu.Lock()
			restarting := pm.restarting
			pm.mu.Unlock()
			if restarting {
				log.Println("claude exited during restart, waiting for new process...")
				time.Sleep(500 * time.Millisecond)
				continue
			}
			log.Println("claude exited, shutting down")
			ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
			_ = httpServer.Shutdown(ctx)
			cancel()
			os.Exit(0)

		case sig := <-sigCh:
			log.Printf("received %v, stopping", sig)
			_ = pm.stopClaude()
			ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
			_ = httpServer.Shutdown(ctx)
			cancel()
			os.Exit(0)
		}
	}
}
