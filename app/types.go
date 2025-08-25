package main

import (
	"net"
	"time"
)

// Entry represents a key-value pair with minimal fields
// more fields will be added as necessary
type Entry struct {
	value     string
	expiresAt time.Time
}

// ListEntry represents a list data structure
type ListEntry struct {
	elements  []string
	expiresAt time.Time
}

// StreamEntry represents a Redis stream data structure
type StreamEntry struct {
	entries   []StreamEntryData
	expiresAt time.Time
}

// StreamEntryData represents a single entry within a stream
type StreamEntryData struct {
	id   string
	data map[string]string // key-value pairs for the entry
}

// BlockedClient represents a client blocked on BLPOP
type BlockedClient struct {
	conn      net.Conn
	listKey   string
	timeout   float64
	startTime time.Time
	done      chan struct{} // channel to signal when client should stop blocking
}

// CommandHandler defines the signature for all command handler functions
type CommandHandler func(args []string, conn net.Conn)
