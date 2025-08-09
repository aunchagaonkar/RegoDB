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

var DB sync.Map

func Start() {
	DB = sync.Map{}
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
		go handleCommand(conn)
	}
}

func handleCommand(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		// remove trailing spaces and newlines
		line = strings.TrimSpace(line)

		if !strings.HasPrefix(line, "*") {
			writeError(conn, "protocol error '"+line+"'")
		}

		// get the number of arguments
		argCount, err := strconv.Atoi(line[1:])

		if err != nil || argCount < 1 {
			writeError(conn, "invalid array")
			return
		}

		// read the arguments
		args := make([]string, 0, argCount)

		// read each argument
		// each argument starts with a '$' followed by the length of the string
		// and ends with '\r\n'
		for i := 0; i < argCount; i++ {
			// read the length line
			lenLine, err := reader.ReadString('\n')
			if err != nil || !strings.HasPrefix(lenLine, "$") {
				writeError(conn, "invalid bulk string")
				return
			}
			// remove the '$' and trailing spaces
			strLen, err := strconv.Atoi(strings.TrimSpace(lenLine[1:]))
			if err != nil {
				writeError(conn, "invalid bulk string length")
				return
			}
			// read the actual string data
			buf := make([]byte, strLen+2)
			_, err = reader.Read(buf)
			if err != nil {
				writeError(conn, "failed to read argument")
				return
			}
			// append the string to args

			args = append(args, string(buf[:strLen]))
		}
		// get the command
		command := strings.ToUpper(args[0])

		switch command {
		case "PING":
			writeSimpleString(conn, "PONG")
		case "ECHO":
			if len(args) < 2 {
				writeError(conn, "wrong number of arguments for 'echo' command")
			} else {
				writeBulkString(conn, args[1])
			}
		case "SET":
			if len(args) < 3 {
				writeError(conn, "wrong number of arguments for 'set' command")
			} else {
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
		case "GET":
			if len(args) < 2 {
				writeError(conn, "wrong number of arguments for 'get' command")
				continue
			}
			key := args[1]
			value, ok := DB.Load(key)
			if !ok {
				writeNullBulkString(conn)
				continue
			}

			entry := value.(Entry)
			if !entry.expiresAt.IsZero() && time.Now().After(entry.expiresAt) {
				DB.Delete(key)
				writeNullBulkString(conn)
				continue
			}

			writeBulkString(conn, entry.value)
		case "RPUSH":
			if len(args) < 3 {
				writeError(conn, "wrong number of arguments for 'rpush' command")
				continue
			}
			key := args[1]
			element := args[2]

			value, exists := DB.Load(key)
			var listEntry ListEntry

			if exists {
				var ok bool
				listEntry, ok = value.(ListEntry)
				if !ok {
					writeError(conn, "WRONGTYPE Operation against a key holding the wrong kind of value")
					continue
				}
			} else {
				// key doesn't exist, create new list
				listEntry = ListEntry{elements: make([]string, 0)}
			}

			// append element to the list
			listEntry.elements = append(listEntry.elements, element)
			DB.Store(key, listEntry)

			// return the number of elements in the list
			writeInteger(conn, len(listEntry.elements))
		}
	}
}
