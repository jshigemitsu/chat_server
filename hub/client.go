package hub

import (
	"bufio"
	"log"
	"net"
	"strings"
	"github.com/google/uuid"
)

// establish buffer size for disconnection
const sendBufferSize = 16

// initialize new clients
func NewClient(conn net.Conn) *Client {
	c := &Client{
		ID:    uuid.New().String(),
		Conn:  conn,
		Send:  make(chan []byte, sendBufferSize),
		Rooms: make(map[string]bool),
	}
	c.SetNick("anonymous")
	return c
}

// establish entry point connection for a client
func HandleConnection(conn net.Conn, h *Hub) {
	client := NewClient(conn)
	h.register <- client

	go writeLoop(client)
	readLoop(client, h)
}

// drain the clients outbound channel and 
// write message to socket
func writeLoop(c *Client) {
	for msg := range c.Send {
		_, err := c.Conn.Write(msg)
		if err != nil {
			return
		}
	}
}

// parse each reading line and create an event
func readLoop(c *Client, h *Hub) {
	defer func() {
		h.unregister <- c
		c.Conn.Close()
	}()

	scanner := bufio.NewScanner(c.Conn)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		handleCommand(c, h, line)
	}
	if err := scanner.Err(); err != nil {
		log.Printf("Client %s: read error : %v", c.ID, err)
	}
}

// go through one line and dispatch it to the hub
func handleCommand(c *Client, h *Hub, line string) {
	parts := strings.SplitN(line, " ", 2)
	cmd := strings.ToUpper(parts[0])

	switch cmd {
	case "NICK":
		if len(parts) < 2 {
			return
		}
		nick := strings.TrimSpace(parts[1])
		if nick == "" {
			return
		}
		h.nick <- nickRequest{client: c, nick: nick}

	case "JOIN":
		if len(parts) < 2 {
			return
		}
		room := strings.TrimSpace(parts[1])
		h.join <- joinRequest{client: c, room: room}

	case "PART":
		if len(parts) < 2 {
			return
		}
		room := strings.TrimSpace(parts[1])
		h.part <- partRequest{client: c, room: room}

	case "MSG":
		if len(parts) < 2 {
			return
		}
		msgParts := strings.SplitN(parts[1], " ", 2)
		if len(msgParts) < 2 {
			return
		}
		room := msgParts[0]
		text := msgParts[1]
		h.broadcast <- broadcastMessage{sender: c, room: room, text: text}

	default:
	}

}
