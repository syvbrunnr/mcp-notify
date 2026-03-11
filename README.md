# mcp-notify

A transparent notification proxy for MCP (Model Context Protocol) servers. Adds notification support to any MCP client that doesn't natively implement it.

## Problem

MCP servers can emit JSON-RPC notifications (`notifications/resources/list_changed`, `notifications/tools/list_changed`, etc.), but most MCP clients ignore them. This means real-time events (new messages, config changes, tool updates) go unnoticed until the user manually checks.

## Solution

`mcp-notify` sits between the MCP client and its servers:

1. **Proxy layer** (`mcp-notify-proxy`) wraps each stdio MCP server, intercepting outbound notifications and forwarding them to a central hub
2. **Hub** (`mcp-notify`) aggregates notifications from all proxied servers, exposes them via MCP tools, and nudges the client via PTY stdin injection

```
┌─────────────────────────────────────────────────────┐
│ mcp-notify (hub + process manager)                  │
│                                                     │
│   ┌──────────┐    ┌──────────┐    ┌──────────┐     │
│   │ proxy    │    │ proxy    │    │ proxy    │     │
│   │ server-a │    │ server-b │    │ server-c │     │
│   └────┬─────┘    └────┬─────┘    └────┬─────┘     │
│        │               │               │           │
│        └───────────────┼───────────────┘           │
│                        ▼                            │
│                   HTTP hub (:9781)                   │
│                        │                            │
│                        ▼                            │
│              PTY stdin injection                    │
│              "You have new notifications"           │
│                        │                            │
│                        ▼                            │
│                MCP client (child)                   │
└─────────────────────────────────────────────────────┘
```

## Installation

```bash
go install github.com/Vegard-/mcp-notify/cmd/mcp-notify@latest
go install github.com/Vegard-/mcp-notify/cmd/mcp-notify-proxy@latest
```

Or build from source:

```bash
git clone https://github.com/Vegard-/mcp-notify.git
cd mcp-notify
go build -o bin/ ./cmd/mcp-notify/
go build -o bin/ ./cmd/mcp-notify-proxy/
```

## Usage

### With Claude Code

```bash
mcp-notify --mcp-config ~/.claude/mcp.json -- --continue
```

This will:
1. Read your MCP config and wrap each stdio server with `mcp-notify-proxy`
2. Add an `mcp-notify` HTTP MCP server for tool access
3. Start Claude Code with the rewritten config
4. Nudge Claude via PTY when notifications arrive

### With other MCP clients

`mcp-notify` works with any MCP client that runs as a CLI process (Codex, etc.). The proxy layer is fully generic — it intercepts standard JSON-RPC notifications regardless of the client.

For non-Claude clients, you'll need to:
1. Rewrite your MCP config manually (or use `mcp-notify` with `--mcp-config` to generate one)
2. Launch your client as the child process after `--`

```bash
# Example with a custom client
mcp-notify --mcp-config config.json -- your-mcp-client --flags
```

The rewritten config wraps each stdio server with the proxy and adds the hub as an HTTP MCP server. Your client can then call `get_notifications` to retrieve buffered notifications.

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--mcp-config` | | Path to MCP config JSON (required) |
| `--port` | `9781` | Hub HTTP port |
| `--proxy-bin` | auto-detect | Path to `mcp-notify-proxy` binary |
| `--skip-permissions` | `false` | Pass `--dangerously-skip-permissions` to child process |

Everything after `--` is passed through to the child process.

### MCP Tools

Once running, the client has access to these tools via the `mcp-notify` MCP server:

- **`get_notifications`** — Returns and clears all buffered notifications
- **`restart_claude`** — Restart the child process with session resume, token rotation, and optional message injection
- **`wrapper_status`** — Show process state, token source, and notification count

### Config Rewriting

Given an input MCP config like:

```json
{
  "mcpServers": {
    "matrix": {
      "command": "node",
      "args": ["server.js"]
    },
    "remote": {
      "type": "http",
      "url": "https://example.com/mcp"
    }
  }
}
```

`mcp-notify` rewrites stdio servers to use the proxy:

```json
{
  "mcpServers": {
    "matrix": {
      "command": "mcp-notify-proxy",
      "args": ["--hub", "http://localhost:9781", "--name", "matrix", "--", "node", "server.js"]
    },
    "remote": {
      "type": "http",
      "url": "https://example.com/mcp"
    },
    "mcp-notify": {
      "type": "http",
      "url": "http://localhost:9781/mcp"
    }
  }
}
```

HTTP MCP servers are left untouched. The hub is added as an HTTP MCP server so the client can call `get_notifications`.

## Architecture

### Two binaries

- **`mcp-notify`** — The main process. Manages the child process lifecycle (start, stop, restart), runs the notification hub HTTP server, serves MCP tools, and handles PTY stdin multiplexing.
- **`mcp-notify-proxy`** — A lightweight per-server proxy. Spawns the real MCP server, pipes stdin through transparently, and scans stdout for JSON-RPC notifications to forward to the hub.

### Notification flow

1. MCP server emits a JSON-RPC notification on stdout
2. `mcp-notify-proxy` intercepts it, forwards to hub via HTTP POST, and passes it through to the client
3. Hub stores the notification and injects a nudge message into the client's PTY stdin
4. Client sees the nudge and calls `get_notifications` to retrieve the details

### Startup behavior

On startup, MCP servers often emit a burst of notifications as they sync initial state. To avoid flooding the client, the hub suppresses nudges for the first 3 seconds. Notifications are still collected — they just don't trigger a nudge until the grace period ends.

### Process management

`mcp-notify` manages the child process with PTY stdin. This enables:

- **Notification nudging** — inject text into the client's input stream
- **Hot restart** — `restart_claude` tool gracefully stops the process (SIGINT → SIGTERM → SIGKILL) and starts a new instance with session resume
- **Token rotation** — reads OAuth tokens from file on each restart

## Token Rotation

On startup and restart, `mcp-notify` checks for an active token at `$DATA_DIR/.claude-active-token` (defaults to `~/.mcp-data/r7-tools/.claude-active-token`). If found, it's injected as `CLAUDE_CODE_OAUTH_TOKEN`. Falls back to the environment variable.

## Testing

```bash
go test ./...
```

## License

MIT
