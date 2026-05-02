// basic — demonstrates every gsocketio API in one file.
// Run:  go run examples/basic/main.go
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/shishir1290/gsocketio"
)

func main() {
	// ── 1. Create server ──────────────────────────────────────────────────────
	srv := gsocketio.New(&gsocketio.Options{
		PingInterval: 30 * time.Second,
		PingTimeout:  10 * time.Second,
		MaxPayload:   2_000_000,
	})

	// ── 2. Default namespace "/" ───────────────────────────────────────────────

	srv.OnConnect("/", func(c gsocketio.Conn) error {
		fmt.Printf("[/] connect  id=%s\n", c.ID())

		// Store arbitrary data on the connection.
		c.SetContext(map[string]string{"joined": time.Now().Format(time.RFC3339)})

		// Join a room on connect.
		c.Join("lobby")

		// Send a welcome event back to this one client.
		c.Emit("welcome", map[string]string{ //nolint:errcheck
			"message": "Hello from gsocketio!",
			"id":      c.ID(),
		})
		return nil
	})

	srv.OnDisconnect("/", func(c gsocketio.Conn, reason string) {
		fmt.Printf("[/] disconnect id=%s reason=%s\n", c.ID(), reason)
		fmt.Printf("    context was: %v\n", c.Context())
	})

	srv.OnError("/", func(c gsocketio.Conn, err error) {
		fmt.Printf("[/] error id=%s: %v\n", c.ID(), err)
	})

	// Handle "message" events.
	srv.OnEvent("/", "message", func(c gsocketio.Conn, args []json.RawMessage) {
		var text string
		if len(args) > 0 {
			json.Unmarshal(args[0], &text) //nolint:errcheck
		}
		fmt.Printf("[/] message from %s: %q\n", c.ID(), text)

		// Echo back to the sender only.
		c.Emit("echo", text) //nolint:errcheck

		// Broadcast to everyone else in "lobby" (skip sender).
		srv.ToRoom("/", "lobby", "broadcast", c, fmt.Sprintf("%s says: %s", c.ID(), text))
	})

	// Handle "join-room" events.
	srv.OnEvent("/", "join-room", func(c gsocketio.Conn, args []json.RawMessage) {
		var room string
		if len(args) > 0 {
			json.Unmarshal(args[0], &room) //nolint:errcheck
		}
		c.Join(room)
		fmt.Printf("[/] %s joined room %q — members: %d\n", c.ID(), room, srv.RoomLen("/", room))
		c.Emit("joined", room) //nolint:errcheck
	})

	// Handle "leave-room" events.
	srv.OnEvent("/", "leave-room", func(c gsocketio.Conn, args []json.RawMessage) {
		var room string
		if len(args) > 0 {
			json.Unmarshal(args[0], &room) //nolint:errcheck
		}
		c.Leave(room)
		fmt.Printf("[/] %s left room %q\n", c.ID(), room)
		c.Emit("left", room) //nolint:errcheck
	})

	// Handle "rooms" — ask the server for the list of rooms you're in.
	srv.OnEvent("/", "rooms", func(c gsocketio.Conn, _ []json.RawMessage) {
		c.Emit("rooms", c.Rooms()) //nolint:errcheck
	})

	// ── 3. Custom namespace "/admin" ───────────────────────────────────────────

	srv.OnConnect("/admin", func(c gsocketio.Conn) error {
		fmt.Printf("[/admin] connect id=%s\n", c.ID())
		return nil
	})

	srv.OnEvent("/admin", "shutdown", func(c gsocketio.Conn, _ []json.RawMessage) {
		fmt.Printf("[/admin] shutdown requested by %s\n", c.ID())
		// Broadcast alert to all "/" users.
		srv.ToNamespace("/", "alert", "Server shutting down!")
	})

	// ── 4. Background broadcast (heartbeat) ───────────────────────────────────
	go func() {
		tick := time.NewTicker(30 * time.Second)
		defer tick.Stop()
		for range tick.C {
			srv.ToNamespace("/", "heartbeat", map[string]interface{}{
				"time":        time.Now().Format(time.RFC3339),
				"connections": srv.Count(),
			})
		}
	}()

	// ── 5. Start serving ──────────────────────────────────────────────────────
	go srv.Serve() //nolint:errcheck

	mux := http.NewServeMux()
	mux.Handle("/socket.io/", srv)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintf(w, "gsocketio basic example — connect a WebSocket client to ws://localhost:9000/socket.io/?EIO=4&transport=websocket\n")
		fmt.Fprintf(w, "Active connections: %d\n", srv.Count())
		fmt.Fprintf(w, "Rooms in /: %v\n", srv.Rooms("/"))
	})

	log.Println("basic example listening on http://localhost:9000")
	log.Fatal(http.ListenAndServe(":9000", mux))
}
