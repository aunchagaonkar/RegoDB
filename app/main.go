package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// this is a struct that represents the key-value pair with minimal fields
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

// BlockedClient represents a client blocked on BLPOP
type BlockedClient struct {
	conn      net.Conn
	listKey   string
	timeout   float64
	startTime time.Time
	done      chan struct{} // channel to signal when client should stop blocking
}

var DB sync.Map

// blockedClients stores clients blocked on BLPOP, organized by list key
var blockedClients = make(map[string][]*BlockedClient)
var blockedClientsMutex sync.RWMutex

func Start() {
	DB = sync.Map{}
}

// CommandHandler defines the signature for all command handler functions
type CommandHandler func(args []string, conn net.Conn)

// Map of command names to their handler functions
var commandHandlers = map[string]CommandHandler{
	"PING":   handlePing,
	"ECHO":   handleEcho,
	"SET":    handleSet,
	"GET":    handleGet,
	"RPUSH":  handleRPush,
	"LRANGE": handleLRange,
	"LLEN":   handleLLen,
	"LPUSH":  handleLPush,
	"LPOP":   handleLPop,
	"BLPOP":  handleBLPop,
}

// RESP protocol response helpers
func writeSimpleString(conn net.Conn, str string) error {
	_, err := conn.Write([]byte("+" + str + "\r\n"))
	return err
}

func writeBulkString(conn net.Conn, str string) error {
	response := fmt.Sprintf("$%d\r\n%s\r\n", len(str), str)
	_, err := conn.Write([]byte(response))
	return err
}

func writeNullBulkString(conn net.Conn) error {
	_, err := conn.Write([]byte("$-1\r\n"))
	return err
}

func writeInteger(conn net.Conn, val int) error {
	_, err := conn.Write([]byte(fmt.Sprintf(":%d\r\n", val)))
	return err
}

func writeError(conn net.Conn, msg string) error {
	_, err := conn.Write([]byte("-ERR " + msg + "\r\n"))
	return err
}

// function to write an RESP array
func writeArray(conn net.Conn, elems []string) error {
	out := fmt.Sprintf("*%d\r\n", len(elems))
	for _, e := range elems {
		out += fmt.Sprintf("$%d\r\n%s\r\n", len(e), e)
	}
	_, err := conn.Write([]byte(out))
	return err
}

func main() {
	fmt.Println("Logs from your program will appear here!")
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	// start the db
	go Start()

	// Accepting a connection to keep the server running
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		// handle commands
		go handleConnection(conn)
	}
}

// parseRESPArray parses a RESP array and returns the arguments
func parseRESPArray(reader *bufio.Reader) ([]string, error) {
	// Read the array header line
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimSpace(line)

	if !strings.HasPrefix(line, "*") {
		return nil, fmt.Errorf("protocol error: expected array, got '%s'", line)
	}

	// Parse array length
	argCount, err := strconv.Atoi(line[1:])
	if err != nil || argCount < 1 {
		return nil, fmt.Errorf("invalid array length")
	}

	// Read each bulk string in the array
	args := make([]string, 0, argCount)
	for i := 0; i < argCount; i++ {
		// Read the bulk string header
		lenLine, err := reader.ReadString('\n')
		if err != nil || !strings.HasPrefix(lenLine, "$") {
			return nil, fmt.Errorf("invalid bulk string header")
		}

		// Parse bulk string length
		strLen, err := strconv.Atoi(strings.TrimSpace(lenLine[1:]))
		if err != nil {
			return nil, fmt.Errorf("invalid bulk string length")
		}

		// read the actual string data
		buf := make([]byte, strLen+2)
		// +2 for CRLF - (Carriage Return Line Feed) i.e. \r\n
		_, err = reader.Read(buf)
		if err != nil {
			return nil, fmt.Errorf("failed to read argument data")
		}

		args = append(args, string(buf[:strLen]))
	}

	return args, nil
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		args, err := parseRESPArray(reader)
		if err != nil {
			if err.Error() != "EOF" {
				writeError(conn, err.Error())
			}
			return
		}

		if len(args) == 0 {
			writeError(conn, "empty command")
			continue
		}

		command := strings.ToUpper(args[0])
		handler, exists := commandHandlers[command]

		if exists {
			handler(args, conn)
		} else {
			writeError(conn, fmt.Sprintf("unknown command '%s'", command))
		}
	}
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
