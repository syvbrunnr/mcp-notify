package proxy

import (
	"encoding/json"
	"testing"
)

func TestIsNotification(t *testing.T) {
	tests := []struct {
		name string
		msg  JSONRPCMessage
		want bool
	}{
		{
			name: "notification with method",
			msg:  JSONRPCMessage{JSONRPC: "2.0", Method: "notifications/resources/updated"},
			want: true,
		},
		{
			name: "request with id",
			msg:  JSONRPCMessage{JSONRPC: "2.0", ID: json.RawMessage(`1`), Method: "tools/call"},
			want: false,
		},
		{
			name: "response with result",
			msg:  JSONRPCMessage{JSONRPC: "2.0", ID: json.RawMessage(`1`), Result: json.RawMessage(`{}`)},
			want: false,
		},
		{
			name: "empty method no id",
			msg:  JSONRPCMessage{JSONRPC: "2.0"},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.msg.IsNotification()
			if got != tt.want {
				t.Errorf("IsNotification() = %v, want %v", got, tt.want)
			}
		})
	}
}
