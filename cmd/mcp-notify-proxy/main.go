package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/syvbrunnr/mcp-notify/internal/proxy"
)

func main() {
	log.SetOutput(os.Stderr)
	log.SetPrefix("[mcp-notify-proxy] ")

	hub := flag.String("hub", "http://localhost:9781", "Hub URL for notification delivery")
	name := flag.String("name", "unknown", "Name of the MCP server being proxied")
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		fmt.Fprintf(os.Stderr, "usage: mcp-notify-proxy --hub URL --name NAME -- command [args...]\n")
		os.Exit(1)
	}

	p := proxy.New(*hub, *name)
	if err := p.Run(args[0], args[1:]); err != nil {
		log.Fatalf("proxy: %v", err)
	}
}
