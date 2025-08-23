package main

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
)

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
