package main

import (
	"net"
	"strconv"
	"strings"
	"time"
)

// Map of command names to their handler functions
var commandHandlers = map[string]CommandHandler{
	"PING":   handlePing,
	"ECHO":   handleEcho,
	"SET":    handleSet,
	"GET":    handleGet,
	"TYPE":   handleType,
	"RPUSH":  handleRPush,
	"LRANGE": handleLRange,
	"LLEN":   handleLLen,
	"LPUSH":  handleLPush,
	"LPOP":   handleLPop,
	"BLPOP":  handleBLPop,
	"XADD":   handleXAdd,
}

// Command handlers
func handlePing(args []string, conn net.Conn) {
	writeSimpleString(conn, "PONG")
}

func handleEcho(args []string, conn net.Conn) {
	if len(args) < 2 {
		writeError(conn, "wrong number of arguments for 'echo' command")
		return
	}
	writeBulkString(conn, args[1])
}

func handleSet(args []string, conn net.Conn) {
	if len(args) < 3 {
		writeError(conn, "wrong number of arguments for 'set' command")
		return
	}

	key := args[1]
	value := args[2]

	// check for optional PX argument
	var expiresAt = time.Time{} // zero time. Will not expire by default
	if len(args) > 4 {
		for i := 3; i < len(args)-1; i++ {
			if strings.ToUpper(args[i]) == "PX" {
				ms, err := strconv.Atoi(args[i+1])
				if err != nil {
					writeError(conn, "PX value must be integer")
					return
				}
				expiresAt = time.Now().Add(time.Duration(ms) * time.Millisecond)
			}
		}
	}
	// if no expiration is set, use a zero time.Time value.
	entry := Entry{value: value, expiresAt: expiresAt}
	DB.Store(key, entry)
	writeSimpleString(conn, "OK")
}

func handleGet(args []string, conn net.Conn) {
	if len(args) < 2 {
		writeError(conn, "wrong number of arguments for 'get' command")
		return
	}

	key := args[1]
	value, ok := DB.Load(key)
	if !ok {
		writeNullBulkString(conn)
		return
	}

	entry := value.(Entry)
	if !entry.expiresAt.IsZero() && time.Now().After(entry.expiresAt) {
		DB.Delete(key)
		writeNullBulkString(conn)
		return
	}

	writeBulkString(conn, entry.value)
}

func handleType(args []string, conn net.Conn) {
	if len(args) < 2 {
		writeError(conn, "wrong number of arguments for 'type' command")
		return
	}

	key := args[1]
	value, ok := DB.Load(key)
	if !ok {
		writeSimpleString(conn, "none")
		return
	}

	// determine the type based on the value's type
	switch v := value.(type) {
	case Entry:
		// check if the entry has expired
		if !v.expiresAt.IsZero() && time.Now().After(v.expiresAt) {
			DB.Delete(key)
			writeSimpleString(conn, "none")
			return
		}
		writeSimpleString(conn, "string")
	case ListEntry:
		writeSimpleString(conn, "list")
	case StreamEntry:
		writeSimpleString(conn, "stream")
	default:
		// unknown type
		writeSimpleString(conn, "none")
	}
}

func handleRPush(args []string, conn net.Conn) {
	if len(args) < 3 {
		writeError(conn, "wrong number of arguments for 'rpush' command")
		return
	}

	key := args[1]
	value, exists := DB.Load(key)
	var listEntry ListEntry

	if exists {
		var ok bool
		listEntry, ok = value.(ListEntry)
		if !ok {
			writeError(conn, "WRONGTYPE Operation against a key holding the wrong kind of value")
			return
		}
	} else {
		// key doesn't exist, create new list
		listEntry = ListEntry{elements: make([]string, 0)}
	}

	// Append all elements to the list (support for multiple values)
	for i := 2; i < len(args); i++ {
		listEntry.elements = append(listEntry.elements, args[i])
	}

	DB.Store(key, listEntry)

	// Notify any blocked clients waiting for this list
	notifyBlockedClients(key)

	// return the number of elements in the list
	writeInteger(conn, len(listEntry.elements))
}

// prepends elements to a list
func handleLPush(args []string, conn net.Conn) {
	if len(args) < 3 {
		writeError(conn, "wrong number of arguments for 'lpush' command")
		return
	}

	key := args[1]
	value, exists := DB.Load(key)
	var listEntry ListEntry

	if exists {
		var ok bool
		listEntry, ok = value.(ListEntry)
		if !ok {
			writeError(conn, "WRONGTYPE Operation against a key holding the wrong kind of value")
			return
		}
	} else {
		// key doesn't exist, create new list
		listEntry = ListEntry{elements: make([]string, 0)}
	}

	// prepend all elements to the list (support for multiple values)
	for i := 2; i < len(args); i++ {
		// insert the element at the beginning
		listEntry.elements = append([]string{args[i]}, listEntry.elements...)
	}

	DB.Store(key, listEntry)

	// Notify any blocked clients waiting for this list
	notifyBlockedClients(key)

	// return the number of elements in the list
	writeInteger(conn, len(listEntry.elements))
}

// handleLPop removes and returns the first element of a list
func handleLPop(args []string, conn net.Conn) {
	if len(args) < 2 || len(args) > 3 {
		writeError(conn, "wrong number of arguments for 'lpop' command")
		return
	}

	key := args[1]
	count := 1 // default count is 1

	// parse optional count parameter
	if len(args) == 3 {
		var err error
		count, err = strconv.Atoi(args[2])
		if err != nil || count < 0 {
			writeError(conn, "value is not an integer or out of range")
			return
		}
	}

	// retrieve the list from the DB
	value, exists := DB.Load(key)
	if !exists {
		if len(args) == 3 {
			// when count is specified and key doesn't exist, return empty array
			writeArray(conn, []string{})
		} else {
			// when no count specified and key doesn't exist, return null
			writeNullBulkString(conn)
		}
		return
	}

	listEntry, ok := value.(ListEntry)
	if !ok {
		writeError(conn, "WRONGTYPE Operation against a key holding the wrong kind of value")
		return
	}

	// if the list is empty
	if len(listEntry.elements) == 0 {
		if len(args) == 3 {
			// when count is specified and list is empty, return empty array
			writeArray(conn, []string{})
		} else {
			// when no count specified and list is empty, return null
			writeNullBulkString(conn)
		}
		return
	}

	// determine how many elements to actually remove
	elementsToRemove := min(count, len(listEntry.elements))

	// get the elements to return
	removedElements := listEntry.elements[:elementsToRemove]

	// remove the elements from the slice
	listEntry.elements = listEntry.elements[elementsToRemove:]

	// if the list becomes empty after popping, remove the key from the DB
	if len(listEntry.elements) == 0 {
		DB.Delete(key)
	} else {
		// Otherwise, store the updated list back
		DB.Store(key, listEntry)
	}

	// return response based on whether count was specified
	if len(args) == 3 {
		// when count is specified, always return an array
		writeArray(conn, removedElements)
	} else {
		// when no count specified, return single bulk string
		writeBulkString(conn, removedElements[0])
	}
}

// lists elements of a list between start and stop indexes, also supporting negative indexes
func handleLRange(args []string, conn net.Conn) {
	if len(args) != 4 {
		writeError(conn, "wrong number of arguments for 'lrange' command")
		return
	}

	key := args[1]
	start, err := strconv.Atoi(args[2])
	if err != nil {
		writeError(conn, "invalid start index")
		return
	}
	stop, err := strconv.Atoi(args[3])
	if err != nil {
		writeError(conn, "invalid stop index")
		return
	}

	// retrieve the list from the DB
	value, exists := DB.Load(key)
	if !exists {
		// if list doesn't exist, return an empty array
		writeArray(conn, []string{})
		return
	}

	listEntry, ok := value.(ListEntry)
	if !ok {
		writeError(conn, "WRONGTYPE Operation against a key holding the wrong kind of value")
		return
	}

	elems := listEntry.elements
	listLen := len(elems)

	// handle negative indexes
	if start < 0 {
		start = max(listLen+start, 0)
	}
	if stop < 0 {
		stop = max(listLen+stop, 0)
	}

	// if start index is out of range, return empty array
	if start >= listLen {
		writeArray(conn, []string{})
		return
	}

	// adjust stop index if it exceeds the list length
	if stop >= listLen {
		stop = listLen - 1
	}

	if start > stop {
		writeArray(conn, []string{})
		return
	}

	result := elems[start : stop+1]
	writeArray(conn, result)
}

// returns the number of elements in a list
func handleLLen(args []string, conn net.Conn) {
	if len(args) != 2 {
		writeError(conn, "wrong number of arguments for 'llen' command")
		return
	}
	key := args[1]
	value, exists := DB.Load(key)
	if !exists {
		writeInteger(conn, 0)
		return
	}
	listEntry, ok := value.(ListEntry)
	if !ok {
		writeError(conn, "WRONGTYPE Operation against a key holding the wrong kind of value")
		return
	}
	writeInteger(conn, len(listEntry.elements))
}

// handleBLPop implements the blocking list pop command
func handleBLPop(args []string, conn net.Conn) {
	if len(args) < 3 {
		writeError(conn, "wrong number of arguments for 'blpop' command")
		return
	}

	// parse timeout (last argument) - can be a float
	timeoutStr := args[len(args)-1]
	timeout, err := strconv.ParseFloat(timeoutStr, 64)
	if err != nil {
		writeError(conn, "timeout is not a float or out of range")
		return
	}

	// extract list keys (all arguments except the last one which is timeout)
	listKeys := args[1 : len(args)-1]

	// try to pop from any of the specified lists immediately
	for _, key := range listKeys {
		value, exists := DB.Load(key)
		if !exists {
			continue
		}

		listEntry, ok := value.(ListEntry)
		if !ok {
			writeError(conn, "WRONGTYPE Operation against a key holding the wrong kind of value")
			return
		}

		if len(listEntry.elements) > 0 {
			// pop the first element
			poppedElement := listEntry.elements[0]
			listEntry.elements = listEntry.elements[1:]

			// update or delete the list
			if len(listEntry.elements) == 0 {
				DB.Delete(key)
			} else {
				DB.Store(key, listEntry)
			}

			// return the result immediately
			writeArray(conn, []string{key, poppedElement})
			return
		}
	}

	// no elements available, block the client
	blockClient(conn, listKeys[0], timeout)
}

// handleXAdd implements the XADD command for Redis streams
func handleXAdd(args []string, conn net.Conn) {
	if len(args) < 4 {
		writeError(conn, "wrong number of arguments for 'xadd' command")
		return
	}

	// XADD syntax: XADD key ID field value [field value ...]
	key := args[1]
	entryID := args[2]

	// Check if we have an even number of field-value pairs
	if (len(args)-3)%2 != 0 {
		writeError(conn, "wrong number of arguments for 'xadd' command")
		return
	}

	// Parse field-value pairs
	data := make(map[string]string)
	for i := 3; i < len(args); i += 2 {
		field := args[i]
		value := args[i+1]
		data[field] = value
	}

	// Get or create the stream
	value, exists := DB.Load(key)
	var streamEntry StreamEntry

	if exists {
		var ok bool
		streamEntry, ok = value.(StreamEntry)
		if !ok {
			writeError(conn, "WRONGTYPE Operation against a key holding the wrong kind of value")
			return
		}
	} else {
		// key doesn't exist, create new stream
		streamEntry = StreamEntry{entries: make([]StreamEntryData, 0)}
	}

	// Create new stream entry data
	newEntry := StreamEntryData{
		id:   entryID,
		data: data,
	}

	// Add the entry to the stream
	streamEntry.entries = append(streamEntry.entries, newEntry)

	// Store the updated stream
	DB.Store(key, streamEntry)

	// Return the entry ID as a bulk string
	writeBulkString(conn, entryID)
}
