package main

import (
	"errors"
	"log"
	"net"
	"chatserver/hub"
	"os"
	"os/signal"
	"syscall"
)

const listenAddr = ":6667"

func main() {
	h := hub.NewHub()
	go h.Run()

	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalf("Failed to listen on  %s: %v",listenAddr,err)
	}

	log.Printf("Chat server listening on %s", listenAddr)

	signalChannel := make(chan os.Signal,1)
	signal.Notify(signalChannel, syscall.SIGINT, syscall.SIGTERM)

	shutdownDone := make(chan struct{})

	go func() {
		<- signalChannel
		log.Println("shutdown signal received, no longer accepting new connections")
		listener.Close()
		h.Shutdown()
		log.Println("Shutdown complete")
		close(shutdownDone)
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				break
			}
			log.Printf("Accept error: %v",err)
			continue
		}
		go hub.HandleConnection(conn, h)
	}
	<- shutdownDone
}