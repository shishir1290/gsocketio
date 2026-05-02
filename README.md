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

## Installation

```
go get gsocketio   # (local module — copy the directory into your project)
```

Or just drop the folder alongside your `go.mod` and add:

```
require gsocketio v0.0.0
replace gsocketio => ./gsocketio
```

---

## Quick Start

```go
package main

import (
    "encoding/json"
    "fmt"
    "log"
    "net/http"

    "gsocketio"
)

func main() {
    srv := gsocketio.New(nil)

    srv.OnConnect("/", func(c gsocketio.Conn) error {
        fmt.Println("connected:", c.ID())
        c.Join("lobby")
        c.Emit("welcome", "Hello from gsocketio!")
        return nil
    })

    srv.OnEvent("/", "chat", func(c gsocketio.Conn, args []json.RawMessage) {
        var msg string
        json.Unmarshal(args[0], &msg)
        srv.ToRoom("/", "lobby", "chat", c, msg)   // broadcast, skip sender
    })

    srv.OnDisconnect("/", func(c gsocketio.Conn, reason string) {
        fmt.Println("disconnected:", c.ID(), reason)
    })

    go srv.Serve()
    http.Handle("/socket.io/", srv)
    log.Fatal(http.ListenAndServe(":8080", nil))
}
```

---

## API Reference

### Creating a server

```go
srv := gsocketio.New(nil)   // sensible defaults

srv := gsocketio.New(&gsocketio.Options{
    PingInterval: 25 * time.Second,
    PingTimeout:  20 * time.Second,
    MaxPayload:   1_000_000,           // bytes
})
```

### Mounting on an HTTP mux

```go
mux := http.NewServeMux()
mux.Handle("/socket.io/", srv)          // srv implements http.Handler
http.ListenAndServe(":8080", mux)
```

### Starting the accept loop

```go
go srv.Serve()   // non-blocking; run in a goroutine
```

### Shutting down

```go
srv.Close()
```

---

### Event handlers

#### OnConnect

Called when a client successfully connects to a namespace.
Return a non-nil error to reject the connection.

```go
srv.OnConnect("/", func(c gsocketio.Conn) error {
    if !isAuthorised(c) {
        return errors.New("not authorised")   // client receives CONNECT_ERROR
    }
    c.SetContext(loadUser(c.ID()))
    return nil
})
```

#### OnDisconnect

```go
srv.OnDisconnect("/", func(c gsocketio.Conn, reason string) {
    fmt.Println(c.ID(), "disconnected:", reason)
})
```

#### OnError

```go
srv.OnError("/", func(c gsocketio.Conn, err error) {
    log.Println("socket error:", err)
})
```

#### OnEvent

```go
// fn signature: func(c gsocketio.Conn, args []json.RawMessage)
srv.OnEvent("/", "message", func(c gsocketio.Conn, args []json.RawMessage) {
    var text string
    json.Unmarshal(args[0], &text)
    // args[1], args[2] … for multiple arguments
})
```

Unmarshalling a struct argument:

```go
type ChatMsg struct {
    Room string `json:"room"`
    Text string `json:"text"`
}
srv.OnEvent("/", "chat", func(c gsocketio.Conn, args []json.RawMessage) {
    var m ChatMsg
    json.Unmarshal(args[0], &m)
    fmt.Println(m.Room, m.Text)
})
```

---

### The Conn interface

```go
type Conn interface {
    ID()        string            // unique session id
    Namespace() string            // e.g. "/"
    Emit(event string, args ...interface{}) error
    Join(room string)
    Leave(room string)
    Rooms() []string
    Context() interface{}
    SetContext(v interface{})
    Close() error
}
```

#### Sending events

```go
c.Emit("ping")
c.Emit("welcome", "Hello!")
c.Emit("data", map[string]int{"count": 42})
c.Emit("multi", "arg1", 2, true)   // multiple args
```

#### Context — storing per-connection data

```go
srv.OnConnect("/", func(c gsocketio.Conn) error {
    c.SetContext(&UserSession{ID: parseToken(c)})
    return nil
})

srv.OnEvent("/", "profile", func(c gsocketio.Conn, _ []json.RawMessage) {
    sess := c.Context().(*UserSession)
    c.Emit("profile", sess)
})
```

---

### Namespaces

Register handlers on any namespace:

```go
// Root namespace
srv.OnConnect("/", handlerFn)
srv.OnEvent("/", "event", handlerFn)

// Custom namespace — clients must connect to "/admin" explicitly
srv.OnConnect("/admin", handlerFn)
srv.OnEvent("/admin", "shutdown", handlerFn)
```

Clients connect to a namespace by sending a CONNECT packet with the namespace
name in the Socket.IO handshake.

---

### Rooms

Connections can join multiple rooms. Rooms are created lazily and cleaned up
when empty.

```go
// In a handler:
c.Join("room-name")
c.Leave("room-name")
rooms := c.Rooms()   // []string of current rooms

// Server-side room management:
srv.JoinRoom("/", "room", c)
srv.LeaveRoom("/", "room", c)
srv.LeaveAllRooms("/", c)
srv.ClearRoom("/", "room")           // remove all members

n     := srv.RoomLen("/", "room")    // member count
names := srv.Rooms("/")              // all room names in namespace
conns := srv.RoomMembers("/", "room") // []Conn
srv.ForEachInRoom("/", "room", func(c Conn) { … })
```

---

### Broadcasting

```go
// Everyone in a room (skip sender):
srv.ToRoom("/", "lobby", "chat", c, message)

// Everyone in a room (no skip):
srv.ToRoom("/", "lobby", "announcement", nil, "Server restart in 5 min")

// Everyone connected to a namespace (all rooms):
srv.ToNamespace("/", "alert", "Something happened")
```

---

### Connection count

```go
n := srv.Count()   // live connections across all namespaces
```

---

## Connecting from a browser

The server speaks Socket.IO v4 over WebSocket. Connect with the official
`socket.io-client` library **or** use a raw WebSocket with the EIO4 framing:

```js
// Using official socket.io-client (CDN):
// <script src="https://cdn.socket.io/4.7.2/socket.io.min.js"></script>
const socket = io("http://localhost:8080");
socket.on("connect", () => console.log("connected:", socket.id));
socket.emit("chat", { room: "lobby", text: "Hello!" });
socket.on("chat", (msg) => console.log(msg));
```

Raw WebSocket (no library):

```js
const ws = new WebSocket(
  "ws://localhost:8080/socket.io/?EIO=4&transport=websocket",
);

ws.onmessage = (e) => {
  const type = e.data[0];
  if (type === "0") {
    ws.send("0");
    return;
  } // EIO open → SIO connect
  if (e.data[0] === "2") {
    // SIO event
    const [event, ...args] = JSON.parse(e.data.slice(1));
    console.log(event, args);
  }
};

function emit(event, ...args) {
  ws.send("2" + JSON.stringify([event, ...args]));
}
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

## Running the tests

```bash
go test ./...
```

Expected output:

```
ok  gsocketio/packet     29 tests
ok  gsocketio/rooms      28 tests
ok  gsocketio/transport  13 tests
ok  gsocketio/tests      21 tests
```

**71 tests total, zero external dependencies.**

---

## Running the examples

```bash
# Chat demo (open http://localhost:8080 in two tabs)
go run examples/chat/main.go

# API showcase (http://localhost:9000)
go run examples/basic/main.go
```

---

## Architecture deep-dive

### Layer 1 — `transport`

Implements RFC 6455 (WebSocket) and HTTP long-poll from scratch:

- `readFrame` / `writeFrame` — frame-level I/O with masking support
- `WSConn` — bidirectional WebSocket connection (ping/pong auto-reply, close handshake)
- `PollConn` — channel-backed pseudo-connection for HTTP long-poll
- `Server.ServeHTTP` — HTTP entry point; routes to WS upgrade or poll handler

### Layer 2 — `packet`

Socket.IO v4 packet codec:

- Five packet types: CONNECT, DISCONNECT, EVENT, ACK, CONNECT_ERROR
- Namespace encoding (`/admin,`)
- Ack ID encoding (decimal integer prefix)
- `BuildEventData` / `EventName` / `EventArgs` helpers

### Layer 3 — `rooms`

Concurrent room manager:

- Per-room `sync.RWMutex` for fine-grained locking
- `snapshot()` pattern — copy member list before iterating (prevents deadlock)
- `SendAll` deduplicates across rooms with a `seen` map

### Layer 4 — `server`

Wires everything together:

- `readPump` — decode packets, dispatch events in goroutines
- `writePump` — drain `sendCh` channel to transport
- `teardown` — atomic close flag, room cleanup, OnDisconnect callback
- `namespace` — per-namespace handler registry with `sync.RWMutex`

### Layer 5 — `gsocketio`

Thin facade that re-exports types so users only ever import `"gsocketio"`.
