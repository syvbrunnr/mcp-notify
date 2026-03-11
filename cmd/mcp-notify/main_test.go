package main

import (
	"encoding/json"
	"os"
	"testing"
)

func TestRewriteConfig(t *testing.T) {
	// Create a mock MCP config.
	config := map[string]any{
		"mcpServers": map[string]any{
			"matrix": map[string]any{
				"command": "node",
				"args":    []any{"server.js", "--port", "3000"},
			},
			"r7": map[string]any{
				"command": "./r7-tools",
			},
			"remote-api": map[string]any{
				"type": "http",
				"url":  "https://example.com/mcp",
			},
		},
	}

	data, _ := json.Marshal(config)
	tmpFile, err := os.CreateTemp("", "mcp-config-*.json")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Write(data)
	tmpFile.Close()

	// Run rewriteConfig.
	outPath, err := rewriteConfig(tmpFile.Name(), "/usr/bin/mcp-notify-proxy", 9781)
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(outPath)

	// Read the rewritten config.
	outData, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatal(err)
	}

	var result map[string]any
	if err := json.Unmarshal(outData, &result); err != nil {
		t.Fatal(err)
	}

	servers, ok := result["mcpServers"].(map[string]any)
	if !ok {
		t.Fatal("expected mcpServers map")
	}

	// Check matrix server was wrapped.
	matrix, ok := servers["matrix"].(map[string]any)
	if !ok {
		t.Fatal("expected matrix server")
	}
	if matrix["command"] != "/usr/bin/mcp-notify-proxy" {
		t.Errorf("expected command to be proxy, got %v", matrix["command"])
	}
	args, ok := matrix["args"].([]any)
	if !ok {
		t.Fatal("expected args array")
	}
	// Should be: --hub http://localhost:9781 --name matrix -- node server.js --port 3000
	expectedArgs := []string{"--hub", "http://localhost:9781", "--name", "matrix", "--", "node", "server.js", "--port", "3000"}
	if len(args) != len(expectedArgs) {
		t.Fatalf("expected %d args, got %d: %v", len(expectedArgs), len(args), args)
	}
	for i, expected := range expectedArgs {
		if args[i] != expected {
			t.Errorf("arg[%d]: expected %q, got %v", i, expected, args[i])
		}
	}

	// Check r7 server was wrapped (no original args).
	r7, ok := servers["r7"].(map[string]any)
	if !ok {
		t.Fatal("expected r7 server")
	}
	if r7["command"] != "/usr/bin/mcp-notify-proxy" {
		t.Errorf("expected r7 command to be proxy, got %v", r7["command"])
	}

	// Check HTTP server was NOT wrapped.
	remote, ok := servers["remote-api"].(map[string]any)
	if !ok {
		t.Fatal("expected remote-api server")
	}
	if remote["type"] != "http" {
		t.Errorf("expected remote-api type to stay http, got %v", remote["type"])
	}
	if remote["url"] != "https://example.com/mcp" {
		t.Errorf("expected remote-api url unchanged, got %v", remote["url"])
	}

	// Check mcp-notify was added.
	notify, ok := servers["mcp-notify"].(map[string]any)
	if !ok {
		t.Fatal("expected mcp-notify server added")
	}
	if notify["type"] != "http" {
		t.Errorf("expected mcp-notify type http, got %v", notify["type"])
	}
	if notify["url"] != "http://localhost:9781/mcp" {
		t.Errorf("expected mcp-notify url, got %v", notify["url"])
	}
}
