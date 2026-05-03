package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync/atomic"
	"time"

	sio "github.com/shishir1290/gsocketio"
)

// ChatMessage represents a message sent in a chat room
type ChatMessage struct {
	ID        string `json:"id"`
	UserID    string `json:"userId"`
	Username  string `json:"username"`
	Text      string `json:"text"`
	MediaURL  string `json:"mediaUrl,omitempty"`
	MediaType string `json:"mediaType,omitempty"`
	Room      string `json:"room"`
	Timestamp int64  `json:"timestamp"`
	Type      string `json:"type"` // "user" or "system"
}

// UserInfo represents basic user identity
type UserInfo struct {
	UserID   string `json:"userId"`
	Username string `json:"username"`
}

// JoinLeavePayload represents room join/leave data
type JoinLeavePayload struct {
	Room     string `json:"room"`
	UserID   string `json:"userId"`
	Username string `json:"username"`
}

// TypingPayload represents typing indicator data
type TypingPayload struct {
	Room     string `json:"room"`
	UserID   string `json:"userId"`
	Username string `json:"username"`
	IsTyping bool   `json:"isTyping"`
}

// connUser stores per-connection user data
type connUser struct {
	UserID   string
	Username string
}

func main() {
	// Initialize gsocketio server
	// We use a longer ping timeout to be safe
	srv := sio.New(&sio.Options{
		PingInterval: 25 * time.Second,
		PingTimeout:  60 * time.Second,
		MaxPayload:   1_000_000,
	})

	// --- Connection handler ---
	// The library normalizes namespaces to always start with "/"
	srv.OnConnect("/", func(c sio.Conn) error {
		log.Printf("✅ Client connected: %s (Namespace: %s)", c.ID(), c.Namespace())
		// Join general room
		c.Join("general")

		// Emit dummy event in a goroutine to force a Flush AFTER writePump starts.
		// writePump is started by the library immediately after this handler returns.
		go func() {
			time.Sleep(100 * time.Millisecond)
			c.Emit("connection_established", map[string]string{"status": "ok"})
		}()
		return nil
	})

	// --- User identifies themselves ---
	srv.OnEvent("/", "user_info", func(c sio.Conn, args []json.RawMessage) {
		if len(args) == 0 {
			return
		}
		var info UserInfo
		if err := json.Unmarshal(args[0], &info); err != nil {
			log.Printf("❌ user_info parse error: %v", err)
			return
		}
		c.SetContext(connUser{UserID: info.UserID, Username: info.Username})
		log.Printf("👤 User identified: %s (%s)", info.Username, info.UserID)

		sysMsg := ChatMessage{
			ID:        generateID(),
			UserID:    "system",
			Username:  "System",
			Text:      info.Username + " is now online",
			Room:      "general",
			Timestamp: time.Now().UnixMilli(),
			Type:      "system",
		}
		data, _ := json.Marshal(sysMsg)
		srv.ToRoom("/", "general", "chat_message", c, json.RawMessage(data))
	})

	// --- Chat message handler ---
	srv.OnEvent("/", "chat_message", func(c sio.Conn, args []json.RawMessage) {
		log.Printf("💬 Raw args: %v", args)
		if len(args) == 0 {
			log.Printf("❌ No arguments for chat_message")
			return
		}
		var msg ChatMessage
		if err := json.Unmarshal(args[0], &msg); err != nil {
			log.Printf("❌ chat_message parse error: %v", err)
			return
		}
		log.Printf("💬 [%s] %s: %s", msg.Room, msg.Username, msg.Text)
		data, _ := json.Marshal(msg)
		srv.ToRoom("/", msg.Room, "chat_message", nil, json.RawMessage(data))
	})

	// --- Join room handler ---
	srv.OnEvent("/", "join_room", func(c sio.Conn, args []json.RawMessage) {
		if len(args) == 0 {
			return
		}
		var payload JoinLeavePayload
		if err := json.Unmarshal(args[0], &payload); err != nil {
			return
		}
		c.Join(payload.Room)
		log.Printf("🚪 %s joined room: %s", payload.Username, payload.Room)

		sysMsg := ChatMessage{
			ID:        generateID(),
			UserID:    "system",
			Username:  "System",
			Text:      payload.Username + " joined the room",
			Room:      payload.Room,
			Timestamp: time.Now().UnixMilli(),
			Type:      "system",
		}
		data, _ := json.Marshal(sysMsg)
		srv.ToRoom("/", payload.Room, "chat_message", c, json.RawMessage(data))
		c.Emit("room_joined", payload.Room)
	})

	// --- Leave room handler ---
	srv.OnEvent("/", "leave_room", func(c sio.Conn, args []json.RawMessage) {
		if len(args) == 0 {
			return
		}
		var payload JoinLeavePayload
		if err := json.Unmarshal(args[0], &payload); err != nil {
			return
		}
		sysMsg := ChatMessage{
			ID:        generateID(),
			UserID:    "system",
			Username:  "System",
			Text:      payload.Username + " left the room",
			Room:      payload.Room,
			Timestamp: time.Now().UnixMilli(),
			Type:      "system",
		}
		data, _ := json.Marshal(sysMsg)
		srv.ToRoom("/", payload.Room, "chat_message", c, json.RawMessage(data))
		c.Leave(payload.Room)
		log.Printf("🚶 %s left room: %s", payload.Username, payload.Room)
	})

	// --- Typing indicator handler ---
	srv.OnEvent("/", "typing", func(c sio.Conn, args []json.RawMessage) {
		if len(args) == 0 {
			return
		}
		var payload TypingPayload
		if err := json.Unmarshal(args[0], &payload); err != nil {
			return
		}
		data, _ := json.Marshal(payload)
		srv.ToRoom("/", payload.Room, "typing", c, json.RawMessage(data))
	})

	// --- Disconnect handler ---
	srv.OnDisconnect("/", func(c sio.Conn, reason string) {
		ctx := c.Context()
		if ctx != nil {
			if user, ok := ctx.(connUser); ok {
				log.Printf("❌ User disconnected: %s (%s) — %s", user.Username, user.UserID, reason)
				sysMsg := ChatMessage{
					ID:        generateID(),
					UserID:    "system",
					Username:  "System",
					Text:      user.Username + " went offline",
					Room:      "general",
					Timestamp: time.Now().UnixMilli(),
					Type:      "system",
				}
				data, _ := json.Marshal(sysMsg)
				srv.ToRoom("/", "general", "chat_message", nil, json.RawMessage(data))
				return
			}
		}
		log.Printf("❌ Client disconnected: %s — %s", c.ID(), reason)
	})

	srv.OnError("/", func(c sio.Conn, err error) {
		log.Printf("⚠️  Error: %v", err)
	})

	// Start background serving
	go srv.Serve()

	// Use a standard Mux and let gsocketio handle its own CORS
	mux := http.NewServeMux()
	mux.Handle("/socket.io/", srv)

	// Add a health check and test endpoint
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Type", "application/json")
		fmt.Fprintf(w, `{"status":"ok","time":"%s"}`, time.Now().Format(time.RFC3339))
	})
	mux.HandleFunc("/test", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "test.html")
	})

	log.Println("🚀 Chat server running on :9090")
	log.Fatal(http.ListenAndServe(":9090", mux))
}

var idCounter int64

func generateID() string {
	c := atomic.AddInt64(&idCounter, 1)
	return fmt.Sprintf("%s-%d", time.Now().Format("20060102150405"), c)
}
