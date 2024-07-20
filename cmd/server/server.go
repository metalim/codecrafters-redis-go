package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	fmt.Println("Logs from your program will appear here!")

	port := flag.String("port", "6379", "Port to listen on")
	replicaOf := flag.String("replicaof", "", "Replicate to another server")
	flag.Parse()

	l, err := net.Listen("tcp", "0.0.0.0:"+*port)
	if err != nil {
		fmt.Println("Failed to bind to port " + *port)
		os.Exit(1)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConnection(conn, replicaOf)
	}
}

func handleConnection(conn net.Conn, replicaOf *string) {
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
			_, err = conn.Write([]byte(bulk(val)))

		case "INFO":
			if len(lines) < 4 || lines[4] != "replication" {
				fmt.Println("Invalid command")
				return
			}
			if *replicaOf != "" {
				_, err = conn.Write([]byte(bulk("role:slave")))
			} else {
				_, err = conn.Write([]byte(bulk("role:master\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\nmaster_repl_offset:0")))
			}
		}

		if err != nil {
			fmt.Println("Error writing to connection: ", err.Error())
			return
		}
	}
}

func bulk(val string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(val), val)
}
