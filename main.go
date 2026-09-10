package main

import (
	"log"
	"net"
	"chatserver/hub"
)

const listenAddr = ":6667"

func main() {
	h := hub.NewHub()
	go h.Run()

	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalf("Failed to listen on  %s: %v",listenAddr,err)
	}
	defer listener.Close()

	log.Printf("Chat server listening on %s", listenAddr)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Accept error: %v",err)
			continue
		}
		go hub.HandleConnection(conn, h)
	}
}