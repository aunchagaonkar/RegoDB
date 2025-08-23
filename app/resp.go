package main

import (
	"fmt"
	"net"
)

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

// writeArray writes an RESP array
func writeArray(conn net.Conn, elems []string) error {
	out := fmt.Sprintf("*%d\r\n", len(elems))
	for _, e := range elems {
		out += fmt.Sprintf("$%d\r\n%s\r\n", len(e), e)
	}
	_, err := conn.Write([]byte(out))
	return err
}
