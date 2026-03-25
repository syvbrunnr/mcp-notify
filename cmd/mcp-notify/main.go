// mcp-notify wraps Claude Code with a transparent MCP notification proxy.
// It intercepts MCP server notifications, buffers them, and injects nudge
// messages into Claude's PTY stdin so that Claude can be alerted to new
// notifications without polling. Also supports token rotation and session
// resume on restart.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/creack/pty"
	"github.com/mark3labs/mcp-go/server"
	"github.com/syvbrunnr/mcp-notify/internal/hub"
	"golang.org/x/term"
)

const (
	defaultPort = 9781
	tokenFile   = ".claude-active-token" // relative to DATA_DIR

	stdinBufSize = 4096

	defaultIdleTimeout = 0 // disabled by default
	defaultIdleMessage = "[idle-detector] No terminal activity detected. This is an automated signal, NOT a user response. Resume autonomous work."
	restartMsgDelay       = 5 * time.Second
	restartFlushDelay     = 10 * time.Second
	restartSignalInterval = 5 * time.Second
	shutdownTimeout       = 5 * time.Second
	stopTimeout           = 10 * time.Second
)

func main() {
	log.SetOutput(os.Stderr)
	log.SetPrefix("[mcp-notify] ")

	mcpConfig := flag.String("mcp-config", "", "Path to MCP config JSON")
	port := flag.Int("port", defaultPort, "Hub port")
	proxyBin := flag.String("proxy-bin", "", "Path to mcp-notify-proxy binary")
	skipPerms := flag.Bool("skip-permissions", false, "Pass --dangerously-skip-permissions to Claude")
	idleTimeout := flag.Duration("idle-timeout", defaultIdleTimeout, "Inject wake-up after this duration of no terminal activity (0 = disabled, e.g. 10s)")
	idleMsg := flag.String("idle-message", defaultIdleMessage, "Message to inject when idle timeout fires")
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

	// Start idle detector if configured.
	if *idleTimeout > 0 {
		h.StartIdleDetector(*idleTimeout, *idleMsg)
	}

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
	httpMux.HandleFunc("/test-nudge", h.HandleTestNudge)
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
		log.Printf("stdin is a terminal (fd=%d), setting raw mode", os.Stdin.Fd())
		oldState, err := term.MakeRaw(int(os.Stdin.Fd()))
		if err != nil {
			log.Printf("warning: failed to set raw mode on stdin: %v", err)
		} else {
			defer term.Restore(int(os.Stdin.Fd()), oldState)
		}
	} else {
		log.Printf("WARNING: stdin is NOT a terminal (fd=%d) — raw mode not set, line buffering may be active", os.Stdin.Fd())
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
