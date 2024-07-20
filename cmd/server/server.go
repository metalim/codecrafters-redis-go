package main

import (
	"fmt"
	"net"
	"os"
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
		handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	buf := make([]byte, 1024)
	for {
		_, err := conn.Read(buf)
		if err != nil {
			if err == net.ErrClosed {
				fmt.Println("Connection closed")
				return
			}
			fmt.Println("Error reading from connection: ", err.Error())
			return
		}
		_, err = conn.Write([]byte("+PONG\r\n"))
		if err != nil {
			fmt.Println("Error writing to connection: ", err.Error())
			return
		}
	}
}