package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/creack/pty"
	"github.com/syvbrunnr/mcp-notify/internal/hub"
	"golang.org/x/term"
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
	cmd.Stderr = os.Stderr

	// Token rotation: only inject CLAUDE_CODE_OAUTH_TOKEN if it was already
	// set in the parent environment. The token file can override the value
	// (for rotation), but we don't force-create the env var when it wasn't
	// there — that would override credentials.json auth.
	if os.Getenv("CLAUDE_CODE_OAUTH_TOKEN") != "" {
		token := readActiveToken()
		if token != "" {
			cmd.Env = buildCleanEnv(token)
			log.Printf("token source: %s", tokenSource())
		}
	}

	// Always use PTY — needed for notification nudge injection.
	ptmx, pts, err := pty.Open()
	if err != nil {
		return fmt.Errorf("open pty: %w", err)
	}
	log.Printf("PTY opened: master=%s (fd=%d), slave=%s (fd=%d)", ptmx.Name(), ptmx.Fd(), pts.Name(), pts.Fd())

	// Propagate outer terminal size to inner PTY so Claude's TUI renders correctly.
	// Without this, the inner PTY defaults to 80x24 which breaks Claude's layout,
	// especially in Docker where the PTY chain is longer.
	if term.IsTerminal(int(os.Stdin.Fd())) {
		if sz, err := pty.GetsizeFull(os.Stdin); err == nil {
			log.Printf("PTY size propagated: %dx%d", sz.Cols, sz.Rows)
			_ = pty.Setsize(ptmx, sz)
		}
	} else {
		log.Printf("WARNING: outer stdin is not a terminal — PTY size not propagated")
	}

	cmd.Stdin = pts
	cmd.Stdout = pts // stdout through PTY so Claude sees a real terminal (isatty=true)

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

	// Copy PTY output → real stdout, tracking activity for idle detection.
	go func() {
		buf := make([]byte, stdinBufSize)
		for {
			n, err := ptmx.Read(buf)
			if n > 0 {
				pm.hub.RecordActivity()
				os.Stdout.Write(buf[:n])
			}
			if err != nil {
				return
			}
		}
	}()

	// Inject restart message after delay if needed.
	// Uses char-by-char delivery to simulate real typing.
	if msg != "" {
		go func() {
			time.Sleep(restartMsgDelay)
			full := msg + "\r"
			log.Printf("injecting restart message (%d bytes, char-by-char)", len(full))
			for _, c := range []byte(full) {
				pm.stdinMu.Lock()
				ptmx.Write([]byte{c})
				pm.stdinMu.Unlock()
				time.Sleep(5 * time.Millisecond)
			}
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
					pm.hub.RecordActivity()
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
