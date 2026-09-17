# gsocketio

[![Website](https://img.shields.io/badge/website-gsocketio.vercel.app-blue)](https://gsocketio.vercel.app)

`gsocketio` is a **pure-Go Socket.IO v4 server** for Go applications.

It implements the Socket.IO protocol and Engine.IO v4 transport layer directly with the Go standard library. The library does **not** import Gorilla WebSocket, `nhooyr.io/websocket`, `gobwas/ws`, or any other third-party Go networking/Socket.IO package.

## Status

- Socket.IO protocol: **v5 / Socket.IO v4 clients**
- Engine.IO: **v4**
- Go dependencies: **zero**
- WebSocket: hand-written RFC 6455 implementation
- Long polling: implemented
- Polling → WebSocket upgrade: implemented
- Namespaces: implemented
- Rooms and broadcasting: implemented
- ACKs: implemented
- Binary events: implemented
- Binary ACKs: implemented
- Connection context: implemented
- CORS/preflight: implemented
- Concurrent server operation: supported

The repository CI validates formatting, `go vet`, unit/integration tests, the race detector, a real `socket.io-client` v4 client, WebSocket, polling, transport upgrade, and a temporary Next.js application. The client/Next.js projects are created only inside CI and are **not stored in this repository**.

## Zero third-party Go dependencies

`go.mod` intentionally contains only the module declaration and Go version:

```go
module github.com/shishir1290/gsocketio

go 1.22.2
```

The transport, packet codec, rooms, server, WebSocket framing, and polling implementation are all part of this repository.

## Installation

```bash
go get github.com/shishir1290/gsocketio@latest
```

Then import it:

```go
import sio "github.com/shishir1290/gsocketio"
```

## Minimal server

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
        return c.Emit("welcome", "Hello from Go!")
    })

    srv.OnEvent("/", "chat", func(c sio.Conn, args []json.RawMessage) {
        if len(args) == 0 {
            return
        }

        var message string
        if err := json.Unmarshal(args[0], &message); err != nil {
            return
        }

        srv.ToRoom("/", "lobby", "chat", c, message)
    })

    srv.OnDisconnect("/", func(c sio.Conn, reason string) {
        log.Println("disconnected:", c.ID(), reason)
    })

    http.Handle("/socket.io/", srv)
    log.Fatal(http.ListenAndServe(":8080", nil))
}
```

Run it:

```bash
go run .
```

## Configuration

```go
srv := sio.New(&sio.Options{
    PingInterval: 25 * time.Second,
    PingTimeout:  20 * time.Second,
    MaxPayload:   1_000_000,
})
```

`nil` uses the same defaults.

## Socket.IO client

The server is intended to work with Socket.IO v4 clients:

```javascript
import { io } from "socket.io-client";

const socket = io("http://localhost:8080", {
  auth: { token: "example" },
});

socket.on("connect", () => {
  console.log("connected:", socket.id);
});

socket.on("welcome", message => {
  console.log(message);
});

socket.emit("chat", "Hello!");
```

You can also force a specific Engine.IO transport during testing:

```javascript
io("http://localhost:8080", { transports: ["websocket"] });
io("http://localhost:8080", { transports: ["polling"] });
io("http://localhost:8080", { transports: ["polling", "websocket"] });
```

## Events

Register an event handler:

```go
srv.OnEvent("/", "message", func(c sio.Conn, args []json.RawMessage) {
    // args contains the JSON arguments sent by the client.
})
```

Send an event:

```go
c.Emit("message", "hello", 123, true)
```

## ACKs

Client events carrying an ACK are handled by the server protocol implementation and acknowledged with the event arguments.

For server-to-client ACK callbacks, use the concrete connection implementation through the server package when you need binary ACK payloads. The core protocol supports both regular and binary ACK packets.

## Binary events

Register a binary event handler:

```go
srv.OnBinaryEvent("/", "upload", func(c sio.Conn, args []interface{}, id *int) {
    // Binary placeholders have already been reconstructed.
    _ = c.Emit("upload-complete", "ok")
})
```

The transport supports binary WebSocket frames and Engine.IO polling binary payloads. Binary Socket.IO packets use the Socket.IO attachment/placeholder format.

## Namespaces

```go
srv.OnConnect("/", func(c sio.Conn) error {
    return nil
})

srv.OnConnect("/admin", func(c sio.Conn) error {
    return nil
})

srv.OnEvent("/admin", "status", func(c sio.Conn, args []json.RawMessage) {
    _ = c.Emit("status", "ok")
})
```

A Socket.IO client can multiplex namespaces over the same Engine.IO connection.

## Authentication / connection context

The Socket.IO `auth` object received during namespace connection is stored in the connection context:

```go
srv.OnConnect("/", func(c sio.Conn) error {
    auth := c.Context()
    log.Printf("auth: %#v", auth)
    return nil
})
```

Application data can also be stored with `SetContext`:

```go
srv.OnConnect("/", func(c sio.Conn) error {
    c.SetContext(map[string]string{"user": "123"})
    return nil
})
```

## Rooms

```go
c.Join("lobby")
c.Leave("lobby")

rooms := c.Rooms()

srv.JoinRoom("/", "lobby", c)
srv.LeaveRoom("/", "lobby", c)
srv.LeaveAllRooms("/", c)
srv.ClearRoom("/", "lobby")
```

Query rooms:

```go
count := srv.RoomLen("/", "lobby")
names := srv.Rooms("/")
members := srv.RoomMembers("/", "lobby")
```

Broadcast:

```go
// Send to everyone in the room except c.
srv.ToRoom("/", "lobby", "chat", c, "hello")

// Send to everyone in the room.
srv.ToRoom("/", "lobby", "announcement", nil, "hello all")

// Send to every connection in a namespace.
srv.ToNamespace("/", "announcement", "server message")
```

## Disconnect and errors

```go
srv.OnDisconnect("/", func(c sio.Conn, reason string) {
    log.Println(c.ID(), reason)
})

srv.OnError("/", func(c sio.Conn, err error) {
    log.Println("protocol error:", err)
})
```

Reject a namespace connection by returning an error:

```go
srv.OnConnect("/admin", func(c sio.Conn) error {
    return errors.New("unauthorized")
})
```

Close a connection from the server:

```go
_ = c.Close()
```

## Testing

Run the complete Go test suite:

```bash
go test ./...
```

Run static analysis:

```bash
go vet ./...
```

Run the race detector:

```bash
go test -race ./...
```

Check formatting:

```bash
test -z "$(gofmt -l .)"
```

The GitHub Actions workflow additionally verifies that the repository remains free of third-party Go imports and runs a real Socket.IO v4 client against the library using:

- WebSocket transport
- polling transport
- polling → WebSocket upgrade
- authentication payloads
- events
- Next.js production build

## Repository layout

```text
gsocketio/
├── gsocketio.go
├── go.mod
├── transport/
│   ├── transport.go
│   └── transport_test.go
├── packet/
│   ├── packet.go
│   └── packet_test.go
├── rooms/
│   ├── rooms.go
│   └── rooms_test.go
├── server/
│   └── server.go
├── logger/
│   └── logger.go
├── examples/
│   ├── basic/
│   └── chat/
└── tests/
    ├── protocol_v4_test.go
    └── server_integration_test.go
```

## Protocol references

The implementation targets the Socket.IO protocol used by Socket.IO v4 clients and Engine.IO v4. The authoritative protocol specifications are maintained by the Socket.IO project:

- Socket.IO protocol: https://github.com/socketio/socket.io-protocol
- Engine.IO protocol: https://github.com/socketio/engine.io-protocol

## Versioning

Use semantic version tags:

```bash
git tag v1.0.1
git push origin v1.0.1
```

Install a specific release:

```bash
go get github.com/shishir1290/gsocketio@v1.0.1
```

## License

See [LICENSE](LICENSE).
