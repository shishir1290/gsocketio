# How to Use gsocketio

This document walks you through using the `gsocketio` package step by step,
with complete, runnable code you can copy straight into your project.

---

## Step 1 — Add to your project

Copy the `gsocketio/` folder next to your `go.mod`, then add two lines:

```go
// go.mod
require gsocketio v0.0.0
replace gsocketio => ./gsocketio
```

---

## Step 2 — Minimal server

```go
package main

import (
    "encoding/json"
    "log"
    "net/http"

    "gsocketio"
)

func main() {
    srv := gsocketio.New(nil)   // nil → sensible defaults

    srv.OnConnect("/", func(c gsocketio.Conn) error {
        log.Println("new connection:", c.ID())
        return nil   // return error to reject
    })

    srv.OnEvent("/", "ping", func(c gsocketio.Conn, args []json.RawMessage) {
        c.Emit("pong", "hello back")
    })

    srv.OnDisconnect("/", func(c gsocketio.Conn, reason string) {
        log.Println("disconnected:", c.ID(), reason)
    })

    go srv.Serve()

    mux := http.NewServeMux()
    mux.Handle("/socket.io/", srv)
    log.Fatal(http.ListenAndServe(":8080", mux))
}
```

---

## Step 3 — Handle structured event arguments

```go
type Message struct {
    Room string `json:"room"`
    Text string `json:"text"`
}

srv.OnEvent("/", "chat", func(c gsocketio.Conn, args []json.RawMessage) {
    if len(args) == 0 {
        return
    }
    var msg Message
    if err := json.Unmarshal(args[0], &msg); err != nil {
        return
    }
    log.Printf("chat: room=%s text=%s", msg.Room, msg.Text)
})
```

---

## Step 4 — Rooms and broadcast

```go
srv.OnConnect("/", func(c gsocketio.Conn) error {
    // Join a room on connect
    c.Join("lobby")
    return nil
})

srv.OnEvent("/", "chat", func(c gsocketio.Conn, args []json.RawMessage) {
    var text string
    json.Unmarshal(args[0], &text)

    // Broadcast to lobby — skip sender
    srv.ToRoom("/", "lobby", "chat", c, text)

    // Broadcast to lobby — include everyone (no skip)
    srv.ToRoom("/", "lobby", "notification", nil, "Someone spoke!")
})

srv.OnEvent("/", "join", func(c gsocketio.Conn, args []json.RawMessage) {
    var room string
    json.Unmarshal(args[0], &room)
    c.Join(room)
    log.Println("joined:", room, "members:", srv.RoomLen("/", room))
})

srv.OnEvent("/", "leave", func(c gsocketio.Conn, args []json.RawMessage) {
    var room string
    json.Unmarshal(args[0], &room)
    c.Leave(room)
})
```

---

## Step 5 — Per-connection context

Store arbitrary data — e.g. the authenticated user — on each connection:

```go
type User struct {
    ID   int
    Name string
}

srv.OnConnect("/", func(c gsocketio.Conn) error {
    user, err := authenticateFromCookie(c)
    if err != nil {
        return err   // reject connection
    }
    c.SetContext(user)
    return nil
})

srv.OnEvent("/", "profile", func(c gsocketio.Conn, _ []json.RawMessage) {
    user := c.Context().(*User)
    c.Emit("profile", user)
})
```

---

## Step 6 — Namespaces

```go
// Default namespace
srv.OnConnect("/", func(c gsocketio.Conn) error { return nil })
srv.OnEvent("/", "ping", func(c gsocketio.Conn, _ []json.RawMessage) {
    c.Emit("pong", "from /")
})

// Admin namespace
srv.OnConnect("/admin", func(c gsocketio.Conn) error {
    // Authenticate admin users here
    return nil
})
srv.OnEvent("/admin", "broadcast", func(c gsocketio.Conn, args []json.RawMessage) {
    var msg string
    json.Unmarshal(args[0], &msg)
    // Send to all "/" users from the admin panel
    srv.ToNamespace("/", "announcement", msg)
})
```

Clients connect to a namespace in JavaScript:

```js
// Connect to root
const s1 = io("http://localhost:8080");

// Connect to /admin
const s2 = io("http://localhost:8080/admin");
```

---

## Step 7 — Server-to-client emit

```go
srv.OnConnect("/", func(c gsocketio.Conn) error {
    // Send immediately on connect
    c.Emit("welcome", map[string]string{"id": c.ID()})

    // Send after a delay
    go func() {
        time.Sleep(5 * time.Second)
        c.Emit("reminder", "You have been here 5 seconds")
    }()
    return nil
})
```

---

## Step 8 — Server-wide queries

```go
// Total connected clients (all namespaces)
n := srv.Count()

// Room member count
n := srv.RoomLen("/", "lobby")

// All room names in a namespace
rooms := srv.Rooms("/")

// All Conn values in a room
conns := srv.RoomMembers("/", "lobby")

// Iterate over room members
srv.ForEachInRoom("/", "lobby", func(c gsocketio.Conn) {
    c.Emit("ping")
})
```

---

## Step 9 — Custom options

```go
srv := gsocketio.New(&gsocketio.Options{
    PingInterval: 25 * time.Second,   // how often to heartbeat
    PingTimeout:  20 * time.Second,   // disconnect if no pong within this
    MaxPayload:   4_000_000,          // max frame size in bytes (4 MB)
})
```

---

## Step 10 — Complete working server

```go
package main

import (
    "encoding/json"
    "fmt"
    "log"
    "net/http"
    "time"

    "gsocketio"
)

type ChatMsg struct {
    Room string `json:"room"`
    User string `json:"user"`
    Text string `json:"text"`
}

func main() {
    srv := gsocketio.New(&gsocketio.Options{
        PingInterval: 30 * time.Second,
        PingTimeout:  10 * time.Second,
    })

    srv.OnConnect("/", func(c gsocketio.Conn) error {
        c.Join("general")
        c.Emit("welcome", fmt.Sprintf("Welcome! ID=%s", c.ID()))
        return nil
    })

    srv.OnDisconnect("/", func(c gsocketio.Conn, reason string) {
        log.Printf("disconnect %s: %s\n", c.ID(), reason)
    })

    srv.OnEvent("/", "chat", func(c gsocketio.Conn, args []json.RawMessage) {
        var msg ChatMsg
        if len(args) > 0 {
            json.Unmarshal(args[0], &msg)
        }
        // Broadcast to room, skipping sender
        srv.ToRoom("/", msg.Room, "chat", c, msg)
    })

    srv.OnEvent("/", "join", func(c gsocketio.Conn, args []json.RawMessage) {
        var room string
        json.Unmarshal(args[0], &room)
        c.Join(room)
        c.Emit("joined", room)
    })

    srv.OnEvent("/", "leave", func(c gsocketio.Conn, args []json.RawMessage) {
        var room string
        json.Unmarshal(args[0], &room)
        c.Leave(room)
        c.Emit("left", room)
    })

    go srv.Serve()

    mux := http.NewServeMux()
    mux.Handle("/socket.io/", srv)
    mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
        fmt.Fprintf(w, "connections: %d, rooms: %v\n", srv.Count(), srv.Rooms("/"))
    })

    log.Println("listening on http://localhost:8080")
    log.Fatal(http.ListenAndServe(":8080", mux))
}
```

---

## Browser client (no library)

```html
<!DOCTYPE html>
<html>
<body>
<script>
const ws = new WebSocket('ws://localhost:8080/socket.io/?EIO=4&transport=websocket');
let ready = false;

ws.onmessage = e => {
    const type = e.data[0];
    if (type === '0') { ws.send('0'); return; }   // EIO OPEN → SIO CONNECT "/"
    if (e.data === '0') { ready = true; return; } // SIO CONNECT ack
    if (type === '2') {                            // SIO EVENT
        const [event, ...args] = JSON.parse(e.data.slice(1));
        console.log('event:', event, args);
    }
};

function emit(event, ...args) {
    if (ready) ws.send('2' + JSON.stringify([event, ...args]));
}

// Usage:
// emit('chat', { room: 'general', user: 'Alice', text: 'Hello!' });
// emit('join', 'sports');
// emit('leave', 'sports');
</script>
</body>
</html>
```

---

## Browser client (with socket.io-client library)

```html
<script src="https://cdn.socket.io/4.7.2/socket.io.min.js"></script>
<script>
const socket = io("http://localhost:8080");

socket.on("connect", () => {
    console.log("connected:", socket.id);
    socket.emit("join", "general");
});

socket.on("welcome", msg => console.log(msg));
socket.on("chat", msg => console.log(msg.user + ":", msg.text));

function sendChat(text) {
    socket.emit("chat", { room: "general", user: "Me", text });
}
</script>
```

---

## Error handling patterns

### Reject a connection

```go
srv.OnConnect("/", func(c gsocketio.Conn) error {
    token := extractToken(c)           // your auth logic
    if !validateToken(token) {
        return errors.New("unauthorized")   // sends CONNECT_ERROR to client
    }
    return nil
})
```

### Handle emit errors

```go
srv.OnEvent("/", "ping", func(c gsocketio.Conn, _ []json.RawMessage) {
    if err := c.Emit("pong", "ok"); err != nil {
        log.Println("emit failed:", err)
        // conn may be full or closed — safe to ignore
    }
})
```

### Close a connection from the server

```go
srv.OnEvent("/", "kick", func(c gsocketio.Conn, _ []json.RawMessage) {
    c.Emit("kicked", "goodbye")   // optional last message
    c.Close()
})
```
