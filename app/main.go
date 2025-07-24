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

var DB sync.Map

func Start() {
	DB = sync.Map{}
}

func main() {
	fmt.Println("Logs from program will appear here!")
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	// start the db
	go Start()

	// accepting a connection to keep the server running
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
			conn.Write([]byte("-ERR protocol error '" + line + "'\r\n"))
		}

		// get the number of arguments
		argCount, err := strconv.Atoi(line[1:])

		if err != nil || argCount < 1 {
			conn.Write([]byte("-ERR invalid array\r\n"))
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
				conn.Write([]byte("-ERR invalid bulk string\r\n"))
				return
			}
			// remove the '$' and trailing spaces
			strLen, err := strconv.Atoi(strings.TrimSpace(lenLine[1:]))
			if err != nil {
				conn.Write([]byte("-ERR invalid bulk string length\r\n"))
				return
			}
			// read the actual string data
			buf := make([]byte, strLen+2)
			_, err = reader.Read(buf)
			if err != nil {
				conn.Write([]byte("-ERR failed to read argument\r\n"))
				return
			}
			// append the string to args

			args = append(args, string(buf[:strLen]))
		}
		// get the command
		command := strings.ToUpper(args[0])

		switch command {
		case "PING":
			conn.Write([]byte("+PONG\r\n"))
		case "ECHO":
			if len(args) < 2 {
				conn.Write([]byte("-ERR wrong number of arguments for 'echo' command\r\n"))
			} else {
				response := fmt.Sprintf("$%d\r\n%s\r\n", len(args[1]), args[1])
				conn.Write([]byte(response))
			}
		case "SET":
			if len(args) < 3 {
				conn.Write([]byte("-ERR wrong number of arguments for 'set' command\r\n"))
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
								conn.Write([]byte("-ERR PX value must be integer\r\n"))
								return
							}
							expiresAt = time.Now().Add(time.Duration(ms) * time.Millisecond)
						}
					}
				}
				// if no expiration is set, use a zero time.Time value.
				entry := Entry{value: value, expiresAt: expiresAt}
				DB.Store(key, entry)
				conn.Write([]byte("+OK\r\n"))
			}
		case "GET":
			if len(args) < 2 {
				conn.Write([]byte("-ERR wrong number of arguments for 'get' command\r\n"))
				continue
			}
			key := args[1]
			value, ok := DB.Load(key)
			if !ok {
				conn.Write([]byte("$-1\r\n"))
				continue
			}

			entry := value.(Entry)
			if !entry.expiresAt.IsZero() && time.Now().After(entry.expiresAt) {
				DB.Delete(key)
				conn.Write([]byte("$-1\r\n"))
				continue
			}

			response := fmt.Sprintf("$%d\r\n%s\r\n", len(entry.value), entry.value)
			conn.Write([]byte(response))
		}
	}
}
