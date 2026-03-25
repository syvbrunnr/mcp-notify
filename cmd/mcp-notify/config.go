package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
)

// rewriteConfig reads an MCP config JSON, wraps each stdio server with the
// proxy binary, adds the mcp-notify HTTP server, and writes the result to a
// temp file.
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

// findProxyBinary locates the mcp-notify-proxy binary.
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
