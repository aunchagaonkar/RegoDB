package main

import (
	"net"
	"sync"
	"time"
)

var DB sync.Map

// blockedClients stores clients blocked on BLPOP, organized by list key
var blockedClients = make(map[string][]*BlockedClient)
var blockedClientsMutex sync.RWMutex

// InitDB initializes the database
func InitDB() {
	DB = sync.Map{}
}

// blockClient blocks a client waiting for an element to be available
func blockClient(conn net.Conn, listKey string, timeout float64) {
	client := &BlockedClient{
		conn:      conn,
		listKey:   listKey,
		timeout:   timeout,
		startTime: time.Now(),
		done:      make(chan struct{}),
	}

	// add client to blocked clients list
	blockedClientsMutex.Lock()
	blockedClients[listKey] = append(blockedClients[listKey], client)
	blockedClientsMutex.Unlock()

	// start a goroutine to handle the blocking
	go func() {
		defer func() {
			// remove client from blocked clients when done
			blockedClientsMutex.Lock()
			clients := blockedClients[listKey]
			for i, c := range clients {
				if c == client {
					blockedClients[listKey] = append(clients[:i], clients[i+1:]...)
					if len(blockedClients[listKey]) == 0 {
						delete(blockedClients, listKey)
					}
					break
				}
			}
			blockedClientsMutex.Unlock()
		}()

		if timeout == 0 {
			// block indefinitely
			<-client.done
		} else {
			// block with timeout
			timeoutDuration := time.Duration(timeout * float64(time.Second))
			select {
			case <-client.done:
				// element became available
			case <-time.After(timeoutDuration):
				// timeout reached, send null response
				writeNullBulkString(conn)
			}
		}
	}()
}

// notifyBlockedClients checks if there are blocked clients waiting for the given list key
// and notifies the longest-waiting client
func notifyBlockedClients(listKey string) {
	blockedClientsMutex.Lock()
	defer blockedClientsMutex.Unlock()

	clients, exists := blockedClients[listKey]
	if !exists || len(clients) == 0 {
		return
	}

	// find the longest-waiting client (first in the slice)
	client := clients[0]

	// try to pop an element for this client
	value, exists := DB.Load(listKey)
	if !exists {
		return
	}

	listEntry, ok := value.(ListEntry)
	if !ok || len(listEntry.elements) == 0 {
		return
	}

	// pop the first element
	poppedElement := listEntry.elements[0]
	listEntry.elements = listEntry.elements[1:]

	// update or delete the list
	if len(listEntry.elements) == 0 {
		DB.Delete(listKey)
	} else {
		DB.Store(listKey, listEntry)
	}

	// send response to the blocked client
	writeArray(client.conn, []string{listKey, poppedElement})

	// remove client from blocked clients list
	blockedClients[listKey] = clients[1:]
	if len(blockedClients[listKey]) == 0 {
		delete(blockedClients, listKey)
	}

	// signal the client to stop blocking
	close(client.done)
}
