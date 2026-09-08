package hub

import "net"

// User.
type Client struct {
	ID string
	Nick string
	Conn net.Conn
	Send chan []byte
	Rooms map[string]bool
}

// Room channel
type Room struct {
	Name string
	Clients map[*Client]bool
}

// Data needed to join a room
type joinRequest struct {
	client *Client
	room string
}

// Data needed to leave a room
type partRequest struct {
	client *Client
	room string
}

// user chat message
type broadcastMessage struct {
	sender *Client
	room string
	payload []byte
}

// main dispatcher manages state
type Hub struct {
	clients map[*Client]bool
	rooms map[string]*Room 

	register chan *Client
	unregister chan *Client
	join chan joinRequest
	part chan partRequest
	broadcast chan broadcastMessage
}

// initialize hub
func NewHub() *Hub {
	return &Hub{
		clients:    make(map[*Client]bool),
		rooms:      make(map[string]*Room),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		join:       make(chan joinRequest),
		part:       make(chan partRequest),
		broadcast:  make(chan broadcastMessage),
	}
}

// run hub in a single threaded event loop
func (h *Hub) Run() {
	for {
		select {
		// Add new user
		case c := <-h.register:
			h.clients[c] = true
		
		// remove user
		case c:= <-h.unregister:
			h.removeClient(c)
		
		// attempt to add user to new room
		case req := <-h.join:
			room, ok := h.rooms[req.room]
			if !ok {
				room = &Room{Name: req.room, Clients: make(map[*Client]bool)}
				h.rooms[req.room] = room
			}
			room.Clients[req.client] = true
			req.client.Rooms[req.room] = true

		// attempt to remove user from a room
		case req := <-h.part:
			if room, ok := h.rooms[req.room]; ok{
				delete(room.Clients, req.client)
			}
			delete(req.client.Rooms, req.room)

		// attempt to send message from a user
		// to all other users in that room
		case msg := <-h.broadcast:
			if room, ok := h.rooms[msg.room]; ok {
				for c := range room.Clients {
					if c == msg.sender {
						continue
					}
					select {
					case c.Send <- msg.payload:
					default:
						h.disconnectSlowClient(c)
					}
				}
			}
		}
	}
}

// remove user
func (h *Hub) removeClient(c *Client) {
	if _, ok := h.clients[c]; !ok {
		return
	}
	delete(h.clients, c)
	for roomName := range c.Rooms {
		if room, ok := h.rooms[roomName]; ok {
			delete(room.Clients, c)
		}
	}
	close(c.Send)
}

// remove user if they are not responding quickly enough
func (h *Hub) disconnectSlowClient(c *Client) {
	if _, ok := h.clients[c]; !ok {
		return
	}
	delete(h.clients, c)
	for roomName := range c.Rooms {
		if room, ok := h.rooms[roomName]; ok {
			delete(room.Clients, c)
		}
	}
	close(c.Send)
	c.Conn.Close()
}