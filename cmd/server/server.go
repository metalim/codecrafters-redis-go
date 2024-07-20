package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	buf := make([]byte, 1024)
	mem := map[string]string{}
	exp := map[string]time.Time{}
	for {
		buf = buf[:cap(buf)]
		n, err := conn.Read(buf)
		if err != nil {
			if err == net.ErrClosed {
				fmt.Println("Connection closed")
				return
			}
			fmt.Println("Error reading from connection: ", err.Error())
			return
		}
		buf = buf[:n]
		fmt.Println("Received:\n", string(buf))
		lines := strings.Split(string(buf), "\r\n")
		if len(lines) < 2 {
			fmt.Println("Invalid command")
			return
		}

		command := strings.ToUpper(lines[2])
		switch command {

		case "PING":
			_, err = conn.Write([]byte("+PONG\r\n"))

		case "ECHO":
			if len(lines) < 4 {
				fmt.Println("Invalid command")
				return
			}
			_, err = conn.Write([]byte("+" + lines[4] + "\r\n"))

		case "SET":
			if len(lines) < 6 {
				fmt.Println("Invalid command")
				return
			}
			key, val := lines[4], lines[6]
			mem[key] = val
			if len(lines) > 10 && lines[8] == "px" {
				dur, err := strconv.Atoi(lines[10]) // in ms
				if err != nil {
					fmt.Println("Invalid command")
					return
				}
				exp[key] = time.Now().Add(time.Duration(dur) * time.Millisecond)
			}
			_, err = conn.Write([]byte("+OK\r\n"))

		case "GET":
			if len(lines) < 4 {
				fmt.Println("Invalid command")
				return
			}
			key := lines[4]
			val, ok := mem[key]
			if !ok {
				_, err = conn.Write([]byte("$-1\r\n"))
				break
			}
			if expTime, ok := exp[key]; ok && time.Now().After(expTime) {
				delete(mem, key)
				delete(exp, key)
				_, err = conn.Write([]byte("$-1\r\n"))
				break
			}
			_, err = conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(val), val)))
		}
		if err != nil {
			fmt.Println("Error writing to connection: ", err.Error())
			return
		}
	}
}
