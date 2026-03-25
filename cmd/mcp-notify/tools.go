package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"syscall"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"github.com/syvbrunnr/mcp-notify/internal/hub"
)

// registerTools adds all MCP tools to the server.
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
			"token_source":          tokenSource(),
			"claude_pid":            pid,
			"mcp_port":              port,
			"pending_notifications": h.Count(),
		}
		out, err := json.MarshalIndent(result, "", "  ")
		if err != nil {
			return mcp.NewToolResultText(fmt.Sprintf("error marshaling status: %v", err)), nil
		}
		return mcp.NewToolResultText(string(out)), nil
	})
}
