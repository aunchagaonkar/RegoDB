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

// blockedXReadClients stores clients blocked on XREAD, organized by stream key
var blockedXReadClients = make(map[string][]*BlockedXReadClient)
var blockedXReadClientsMutex sync.RWMutex

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
				// timeout reached, send null array response
				writeNullArray(conn)
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

// blockXReadClient blocks a client waiting for new entries in streams
func blockXReadClient(conn net.Conn, streamKeys []string, streamIDs []string, timeout float64) {
	client := &BlockedXReadClient{
		conn:       conn,
		streamKeys: streamKeys,
		streamIDs:  streamIDs,
		timeout:    timeout,
		startTime:  time.Now(),
		done:       make(chan struct{}),
	}

	// add client to blocked clients list for each stream key
	blockedXReadClientsMutex.Lock()
	for _, key := range streamKeys {
		blockedXReadClients[key] = append(blockedXReadClients[key], client)
	}
	blockedXReadClientsMutex.Unlock()

	// start a goroutine to handle the blocking
	go func() {
		defer func() {
			// remove client from blocked clients when done
			blockedXReadClientsMutex.Lock()
			for _, key := range streamKeys {
				clients := blockedXReadClients[key]
				for i, c := range clients {
					if c == client {
						blockedXReadClients[key] = append(clients[:i], clients[i+1:]...)
						if len(blockedXReadClients[key]) == 0 {
							delete(blockedXReadClients, key)
						}
						break
					}
				}
			}
			blockedXReadClientsMutex.Unlock()
		}()

		if timeout == 0 {
			// block indefinitely
			<-client.done
		} else {
			// block with timeout
			timeoutDuration := time.Duration(timeout * float64(time.Millisecond)) // timeout is in milliseconds for XREAD
			select {
			case <-client.done:
				// new entry became available
			case <-time.After(timeoutDuration):
				// timeout reached, send null array response
				writeNullArray(conn)
			}
		}
	}()
}

// notifyBlockedXReadClients checks if there are blocked XREAD clients waiting for the given stream key
// and notifies all clients that might be interested in the new entry
func notifyBlockedXReadClients(streamKey string, newEntryID string) {
	blockedXReadClientsMutex.Lock()
	defer blockedXReadClientsMutex.Unlock()

	clients, exists := blockedXReadClients[streamKey]
	if !exists || len(clients) == 0 {
		return
	}

	// check each blocked client to see if they should be notified
	for i := len(clients) - 1; i >= 0; i-- {
		client := clients[i]
		
		// find the stream key index to get the corresponding start ID
		var startID string
		for j, key := range client.streamKeys {
			if key == streamKey {
				startID = client.streamIDs[j]
				break
			}
		}

		// check if the new entry ID is greater than the client's start ID
		comp, err := compareEntryIDs(newEntryID, startID)
		if err != nil {
			continue
		}

		if comp > 0 {
			// notify this client by processing their XREAD request
			processBlockedXReadClient(client)
			
			// remove this client from blocked clients for all streams
			for _, key := range client.streamKeys {
				keyClients := blockedXReadClients[key]
				for k, c := range keyClients {
					if c == client {
						blockedXReadClients[key] = append(keyClients[:k], keyClients[k+1:]...)
						if len(blockedXReadClients[key]) == 0 {
							delete(blockedXReadClients, key)
						}
						break
					}
				}
			}
			
			// signal the client to stop blocking
			close(client.done)
		}
	}
}

// processBlockedXReadClient processes the XREAD request for a blocked client
func processBlockedXReadClient(client *BlockedXReadClient) {
	// result array - each element is [stream_key, [[entry_id, [field1, value1, ...]], ...]]
	var resultStreams [][]interface{}

	for i, key := range client.streamKeys {
		startID := client.streamIDs[i]

		// get the stream
		value, exists := DB.Load(key)
		if !exists {
			// stream doesn't exist, skip this stream
			continue
		}

		streamEntry, ok := value.(StreamEntry)
		if !ok {
			continue
		}

		// find entries with ID greater than startID (exclusive)
		var entries []StreamEntryData
		for _, entry := range streamEntry.entries {
			comp, err := compareEntryIDs(entry.id, startID)
			if err != nil {
				continue
			}
			// include only entries with ID > startID (comp > 0)
			if comp > 0 {
				entries = append(entries, entry)
			}
		}

		// only include this stream in the result if it has entries
		if len(entries) > 0 {
			streamResult := []interface{}{key, entries}
			resultStreams = append(resultStreams, streamResult)
		}
	}

	// write the result as a RESP array
	writeArrayHeader(client.conn, len(resultStreams))
	for _, streamResult := range resultStreams {
		// each stream result is [stream_key, [entries...]]
		writeArrayHeader(client.conn, 2)
		
		// write stream key
		writeBulkString(client.conn, streamResult[0].(string))

		// write entries array
		entries := streamResult[1].([]StreamEntryData)
		writeArrayHeader(client.conn, len(entries))
		
		for _, entry := range entries {
			// each entry is [entry_id, [field1, value1, field2, value2, ...]]
			writeArrayHeader(client.conn, 2)
			
			// write entry ID
			writeBulkString(client.conn, entry.id)

			// write entry data as flat array
			writeArrayHeader(client.conn, len(entry.data)*2)
			for field, value := range entry.data {
				writeBulkString(client.conn, field)
				writeBulkString(client.conn, value)
			}
		}
	}
}
