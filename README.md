# gsocketio

A **Socket.IO v4 server** for Go, written entirely from scratch using only the
Go standard library. No external packages, no GitHub forks — every byte of the
WebSocket handshake, frame parser, packet codec, room manager and event
dispatcher is hand-written.

---

## Why gsocketio?

| Feature           | gsocketio               |
| ----------------- | ----------------------- |
| External deps     | **zero**                |
| WebSocket impl    | Pure stdlib RFC 6455    |
| Socket.IO version | **4.x (EIO 4)**         |
| Namespaces        | ✅                      |
| Rooms             | ✅                      |
| Ack callbacks     | ✅                      |
| Context per conn  | ✅                      |
| Thread-safe       | ✅                      |
| Module path       | local module, zero deps |

---

## Part 1 — Use in Any Go Project

### Step 1 — Install the package

Inside any Go project folder:

```bash
go get github.com/shishir1290/gsocketio@latest
```

This updates your `go.mod` and creates `go.sum` automatically.

---

### Step 2 — Minimal server

Create a `main.go` file:

```go
package main

import (
    "encoding/json"
    "log"
    "net/http"

    sio "github.com/shishir1290/gsocketio"
)

func main() {
    srv := sio.New(nil)

    srv.OnConnect("/", func(c sio.Conn) error {
        c.Join("lobby")
        c.Emit("welcome", "Hello!")
        return nil
    })

    srv.OnEvent("/", "chat", func(c sio.Conn, args []json.RawMessage) {
        var msg string
        json.Unmarshal(args[0], &msg)
        srv.ToRoom("/", "lobby", "chat", c, msg)
    })

    srv.OnDisconnect("/", func(c sio.Conn, reason string) {
        log.Println("disconnected:", c.ID())
    })

    go srv.Serve()
    http.Handle("/socket.io/", srv)
    log.Fatal(http.ListenAndServe(":8080", nil))
}
```

---

### Step 3 — Run

```bash
go mod tidy
go run main.go
```

---

## Part 2 — Full API Reference

### Creating a server

```go
// Default options
srv := sio.New(nil)

// Custom options
srv := sio.New(&sio.Options{
    PingInterval: 25 * time.Second,
    PingTimeout:  20 * time.Second,
    MaxPayload:   1_000_000,
})
```

---

### Event handlers

#### OnConnect — called when a client connects

```go
srv.OnConnect("/", func(c sio.Conn) error {
    log.Println("connected:", c.ID())
    c.Join("lobby")
    return nil  // return error to reject the connection
})
```

#### OnDisconnect — called when a client disconnects

```go
srv.OnDisconnect("/", func(c sio.Conn, reason string) {
    log.Println("disconnected:", c.ID(), reason)
})
```

#### OnError — called on protocol errors

```go
srv.OnError("/", func(c sio.Conn, err error) {
    log.Println("error:", err)
})
```

#### OnEvent — called when a client sends an event

```go
srv.OnEvent("/", "chat", func(c sio.Conn, args []json.RawMessage) {
    var msg string
    json.Unmarshal(args[0], &msg)
    log.Println("message:", msg)
})
```

Unmarshal a struct:

```go
type Message struct {
    Room string `json:"room"`
    Text string `json:"text"`
}

srv.OnEvent("/", "chat", func(c sio.Conn, args []json.RawMessage) {
    var m Message
    json.Unmarshal(args[0], &m)
    log.Println(m.Room, m.Text)
})
```

---

### Sending events to the client

```go
c.Emit("ping")
c.Emit("welcome", "Hello!")
c.Emit("data", map[string]int{"count": 42})
c.Emit("multi", "arg1", 2, true)
```

---

### Rooms

```go
// Join / leave from inside a handler
c.Join("room-name")
c.Leave("room-name")
rooms := c.Rooms()  // []string

// Server-side room management
srv.JoinRoom("/", "room", c)
srv.LeaveRoom("/", "room", c)
srv.LeaveAllRooms("/", c)
srv.ClearRoom("/", "room")

// Query
n     := srv.RoomLen("/", "room")       // member count
names := srv.Rooms("/")                 // all room names
conns := srv.RoomMembers("/", "room")   // []Conn

// Iterate
srv.ForEachInRoom("/", "room", func(c sio.Conn) {
    c.Emit("ping")
})
```

---

### Broadcasting

```go
// To a room — skip sender
srv.ToRoom("/", "lobby", "chat", c, message)

// To a room — include everyone
srv.ToRoom("/", "lobby", "alert", nil, "Server restarting")

// To all connections in a namespace
srv.ToNamespace("/", "alert", "Something happened")
```

---

### Context — store data per connection

```go
srv.OnConnect("/", func(c sio.Conn) error {
    c.SetContext("user-data-here")
    return nil
})

srv.OnEvent("/", "profile", func(c sio.Conn, _ []json.RawMessage) {
    data := c.Context().(string)
    c.Emit("profile", data)
})
```

---

### Namespaces

```go
// Root namespace
srv.OnConnect("/", func(c sio.Conn) error { return nil })
srv.OnEvent("/", "ping", func(c sio.Conn, _ []json.RawMessage) {
    c.Emit("pong", "ok")
})

// Custom namespace
srv.OnConnect("/admin", func(c sio.Conn) error { return nil })
srv.OnEvent("/admin", "shutdown", func(c sio.Conn, _ []json.RawMessage) {
    srv.ToNamespace("/", "alert", "Shutting down!")
})
```

---

### Connection count

```go
n := srv.Count()  // all live connections across all namespaces
```

---

### Reject a connection

```go
srv.OnConnect("/", func(c sio.Conn) error {
    if !isAuthenticated(c) {
        return errors.New("unauthorized")  // sends CONNECT_ERROR to client
    }
    return nil
})
```

---

### Close a connection from the server

```go
srv.OnEvent("/", "kick", func(c sio.Conn, _ []json.RawMessage) {
    c.Emit("kicked", "goodbye")
    c.Close()
})
```

---

## Part 3 — Browser Client

### With socket.io-client (recommended)

```html
<script src="https://cdn.socket.io/4.7.2/socket.io.min.js"></script>
<script>
  const socket = io("http://localhost:8080");

  socket.on("connect", () => console.log("id:", socket.id));
  socket.on("welcome", (msg) => console.log(msg));
  socket.on("chat", (msg) => console.log(msg));
  socket.on("disconnect", () => console.log("disconnected"));

  socket.emit("chat", "Hello from browser!");
  socket.emit("join", "sports");
</script>
```

### With raw WebSocket (no library)

```html
<script>
  const ws = new WebSocket(
    "ws://localhost:8080/socket.io/?EIO=4&transport=websocket",
  );
  let ready = false;

  ws.onmessage = (e) => {
    if (e.data[0] === "0") {
      ws.send("0");
      return;
    } // EIO open → SIO connect
    if (e.data === "0") {
      ready = true;
      return;
    } // SIO connect ack
    if (e.data[0] === "2") {
      const [event, ...args] = JSON.parse(e.data.slice(1));
      console.log(event, args);
    }
  };

  function emit(event, ...args) {
    if (ready) ws.send("2" + JSON.stringify([event, ...args]));
  }

  // Usage
  emit("chat", "Hello!");
  emit("join", "sports");
</script>
```

---

## Part 4 — Updating the Package

When you fix bugs or add features:

```bash
# In the gsocketio/ folder
git add .
git commit -m "fix: your change description"
git push

git tag v1.0.1
git push origin v1.0.1
```

Then in any project that uses it:

```bash
go get github.com/shishir1290/gsocketio@v1.0.1
```

---

## Project structure

```
gsocketio/
├── gsocketio.go            ← public API facade (import this)
├── go.mod
│
├── transport/
│   └── transport.go        ← RFC 6455 WebSocket + HTTP long-poll (pure stdlib)
│
├── packet/
│   └── packet.go           ← Socket.IO v4 packet encode/decode
│
├── rooms/
│   └── rooms.go            ← concurrent room manager
│
├── server/
│   └── server.go           ← Conn, Namespace, Server core
│
├── logger/
│   └── logger.go           ← levelled logger
│
├── examples/
│   ├── chat/main.go        ← multi-room chat demo
│   └── basic/main.go       ← every API in one file
│
└── tests/
    └── server_integration_test.go   ← 21 end-to-end tests
        (packet, rooms, transport tests live next to their packages)
```

---

## Quick Reference

| What                | Command                                             |
| ------------------- | --------------------------------------------------- |
| Install             | `go get github.com/shishir1290/gsocketio@v1.0.0`    |
| Install latest      | `go get github.com/shishir1290/gsocketio@latest`    |
| Update version      | `go get github.com/shishir1290/gsocketio@v1.0.1`    |
| Tag new release     | `git tag v1.0.1 && git push origin v1.0.1`          |
| Source code         | https://github.com/shishir1290/gsocketio            |
| Auto-generated docs | https://pkg.go.dev/github.com/shishir1290/gsocketio |

---

## go.mod example

After running `go get`, your project's `go.mod` will look like:

```
module github.com/shishir1290/my-app

go 1.22

require github.com/shishir1290/gsocketio v1.0.0
```
