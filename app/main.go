package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

func main() {
	fmt.Println("Logs from program will appear here!")
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
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
		}

	}
}
