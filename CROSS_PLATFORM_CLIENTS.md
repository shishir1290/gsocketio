# gsocketio — Cross-Platform Client Connection Guide

Your Go backend (`github.com/shishir1290/gsocketio`) speaks **Socket.IO v4 / Engine.IO v4**.
Every client library that speaks this protocol connects to it out of the box.

---

## How it works (EIO4 wire protocol)

```
Client → Server:  HTTP GET /socket.io/?EIO=4&transport=websocket
                  Upgrade: websocket

Server → Client:  101 Switching Protocols
Server → Client:  WS frame: 0{"sid":"...","pingInterval":25000,"pingTimeout":20000}
Client → Server:  WS frame: 40              ← EIO "4" + SIO CONNECT "0" to "/"
Server → Client:  WS frame: 40              ← SIO CONNECT ack
         ... connection is live ...
Server → Client:  WS frame: 2              ← EIO ping (every 25s)
Client → Server:  WS frame: 3              ← EIO pong (within 20s)
Client → Server:  WS frame: 42["chat","hi"] ← EIO "4" + SIO EVENT "2" + args
Server → Client:  WS frame: 42["reply","ok"]
```

---

## JavaScript / React / Next.js / Vue

Install: `npm install socket.io-client`

```javascript
import { io } from "socket.io-client";

const socket = io("http://your-server:8080", {
  transports: ["websocket"],   // skip polling, go straight to WS
  reconnection: true,          // auto-reconnect on drop
  reconnectionDelay: 1000,
  reconnectionAttempts: Infinity,
});

socket.on("connect", () => {
  console.log("connected, id:", socket.id);
});

socket.on("disconnect", (reason) => {
  console.log("disconnected:", reason);
  // socket.io-client reconnects automatically
});

socket.on("welcome", (msg) => console.log(msg));
socket.on("chat", (msg) => console.log(msg.user + ":", msg.text));

// Send events
socket.emit("chat", { room: "general", user: "Alice", text: "Hello!" });
socket.emit("join", "sports");
```

### React component example

```jsx
import { useEffect, useState } from "react";
import { io } from "socket.io-client";

const socket = io("http://localhost:8080", { transports: ["websocket"] });

export default function Chat() {
  const [messages, setMessages] = useState([]);

  useEffect(() => {
    socket.on("chat", (msg) => {
      setMessages((prev) => [...prev, msg]);
    });
    return () => socket.off("chat");
  }, []);

  const send = (text) => socket.emit("chat", { text, user: "Me", room: "general" });

  return (
    <div>
      {messages.map((m, i) => <p key={i}>{m.user}: {m.text}</p>)}
      <button onClick={() => send("Hello!")}>Send</button>
    </div>
  );
}
```

---

## Flutter / Dart

Install in `pubspec.yaml`:
```yaml
dependencies:
  socket_io_client: ^2.0.0
```

```dart
import 'package:socket_io_client/socket_io_client.dart' as IO;

class SocketService {
  late IO.Socket socket;

  void connect() {
    socket = IO.io(
      'http://your-server:8080',
      IO.OptionBuilder()
          .setTransports(['websocket'])     // use WebSocket directly
          .enableReconnection()             // auto-reconnect
          .setReconnectionDelay(1000)
          .setReconnectionAttempts(double.infinity.toInt())
          .build(),
    );

    socket.onConnect((_) {
      print('Connected: ${socket.id}');
    });

    socket.onDisconnect((_) {
      print('Disconnected — will reconnect automatically');
    });

    socket.on('welcome', (msg) => print('Welcome: $msg'));

    socket.on('chat', (data) {
      print('${data['user']}: ${data['text']}');
    });
  }

  void sendChat(String text) {
    socket.emit('chat', {'room': 'general', 'user': 'Flutter', 'text': text});
  }

  void joinRoom(String room) {
    socket.emit('join', room);
  }

  void dispose() {
    socket.disconnect();
    socket.dispose();
  }
}
```

---

## Python

Install: `pip install "python-socketio[client]" websocket-client`

```python
import socketio
import time

sio = socketio.Client(reconnection=True, reconnection_attempts=0)

@sio.event
def connect():
    print(f"Connected: {sio.sid}")

@sio.event
def disconnect():
    print("Disconnected — will reconnect automatically")

@sio.on("welcome")
def on_welcome(msg):
    print(f"Welcome: {msg}")

@sio.on("chat")
def on_chat(data):
    print(f"{data['user']}: {data['text']}")

# Connect — python-socketio speaks EIO4 natively
sio.connect(
    "http://your-server:8080",
    transports=["websocket"],
    socketio_path="/socket.io/",
)

# Send events
sio.emit("chat", {"room": "general", "user": "Python", "text": "Hello!"})
sio.emit("join", "sports")

# Keep running
sio.wait()
```

---

## React Native

Install: `npm install socket.io-client`

```javascript
import { io } from "socket.io-client";
import { useEffect, useRef } from "react";

export default function useSocket(serverUrl) {
  const socketRef = useRef(null);

  useEffect(() => {
    socketRef.current = io(serverUrl, {
      transports: ["websocket"],
      reconnection: true,
      reconnectionDelay: 1000,
      reconnectionAttempts: Infinity,
    });

    socketRef.current.on("connect", () => {
      console.log("connected:", socketRef.current.id);
    });

    socketRef.current.on("chat", (msg) => {
      console.log(msg.user + ":", msg.text);
    });

    return () => {
      socketRef.current.disconnect();
    };
  }, [serverUrl]);

  const emit = (event, data) => socketRef.current?.emit(event, data);
  return { emit };
}
```

---

## Java / Android (OkHttp + manual EIO4)

Install in `build.gradle`:
```groovy
implementation 'com.squareup.okhttp3:okhttp:4.12.0'
```

```java
import okhttp3.*;
import org.json.*;

public class SocketClient {
    private final OkHttpClient client = new OkHttpClient();
    private WebSocket webSocket;

    public void connect(String serverUrl) {
        Request request = new Request.Builder()
            .url(serverUrl + "/socket.io/?EIO=4&transport=websocket")
            .build();

        webSocket = client.newWebSocket(request, new WebSocketListener() {
            @Override
            public void onOpen(WebSocket ws, Response response) {
                // Will receive "0{...}" EIO open packet — wait for it
            }

            @Override
            public void onMessage(WebSocket ws, String text) {
                if (text.isEmpty()) return;
                char type = text.charAt(0);

                switch (type) {
                    case '0': // EIO open — send SIO CONNECT
                        ws.send("40"); // EIO "4" + SIO CONNECT "0"
                        break;
                    case '2': // EIO ping — must reply with pong
                        ws.send("3");
                        break;
                    case '4': // EIO message — contains SIO packet
                        handleSIOPacket(ws, text.substring(1));
                        break;
                }
            }

            @Override
            public void onFailure(WebSocket ws, Throwable t, Response r) {
                System.out.println("Disconnected: " + t.getMessage());
                // Implement reconnect logic here
            }
        });
    }

    private void handleSIOPacket(WebSocket ws, String packet) {
        if (packet.isEmpty()) return;
        char sioType = packet.charAt(0);

        switch (sioType) {
            case '0': // SIO CONNECT ack
                System.out.println("Connected to namespace!");
                break;
            case '2': // SIO EVENT
                try {
                    JSONArray arr = new JSONArray(packet.substring(1));
                    String event = arr.getString(0);
                    System.out.println("Event: " + event + " data: " + arr.opt(1));
                } catch (JSONException e) {
                    e.printStackTrace();
                }
                break;
        }
    }

    // Send a Socket.IO event
    public void emit(String event, Object data) throws JSONException {
        JSONArray arr = new JSONArray();
        arr.put(event);
        arr.put(data);
        // EIO "4" + SIO EVENT "2" + JSON array
        webSocket.send("42" + arr.toString());
    }

    // Join a room
    public void join(String room) throws JSONException {
        emit("join", room);
    }
}
```

---

## Kotlin / Android (recommended)

Install:
```groovy
implementation 'io.socket:socket.io-client:2.1.0'
```

```kotlin
import io.socket.client.IO
import io.socket.client.Socket
import org.json.JSONObject

class SocketService {
    private lateinit var socket: Socket

    fun connect(serverUrl: String) {
        val opts = IO.Options.builder()
            .setTransports(arrayOf("websocket"))
            .setReconnection(true)
            .setReconnectionDelay(1000)
            .build()

        socket = IO.socket(serverUrl, opts)

        socket.on(Socket.EVENT_CONNECT) {
            println("Connected: ${socket.id()}")
        }

        socket.on(Socket.EVENT_DISCONNECT) {
            println("Disconnected — reconnecting automatically")
        }

        socket.on("chat") { args ->
            val data = args[0] as JSONObject
            println("${data.getString("user")}: ${data.getString("text")}")
        }

        socket.connect()
    }

    fun sendChat(text: String) {
        val data = JSONObject().apply {
            put("room", "general")
            put("user", "Kotlin")
            put("text", text)
        }
        socket.emit("chat", data)
    }

    fun disconnect() = socket.disconnect()
}
```

---

## Swift / iOS

Install via SPM:
```swift
.package(url: "https://github.com/socketio/socket.io-client-swift", .upToNextMinor(from: "16.0.1"))
```

```swift
import SocketIO

class SocketService {
    var manager: SocketManager!
    var socket: SocketIOClient!

    func connect(to url: String) {
        manager = SocketManager(
            socketURL: URL(string: url)!,
            config: [
                .log(false),
                .compress,
                .reconnects(true),
                .reconnectWait(1),
                .forceWebsockets(true),
            ]
        )
        socket = manager.defaultSocket

        socket.on(clientEvent: .connect) { _, _ in
            print("Connected:", self.socket.sid ?? "")
        }

        socket.on(clientEvent: .disconnect) { _, _ in
            print("Disconnected — will reconnect")
        }

        socket.on("chat") { data, _ in
            if let msg = data[0] as? [String: Any] {
                print("\(msg["user"] ?? ""): \(msg["text"] ?? "")")
            }
        }

        socket.connect()
    }

    func sendChat(text: String) {
        socket.emit("chat", ["room": "general", "user": "Swift", "text": text])
    }
}
```

---

## Raw WebSocket (any language, no library)

Use this if your language has no Socket.IO client library.
This works in Go, Rust, C#, PHP, Ruby, etc.

```
Step 1: WebSocket connect to:
        ws://your-server:8080/socket.io/?EIO=4&transport=websocket

Step 2: Receive from server:
        "0{"sid":"...","pingInterval":25000,"pingTimeout":20000}"

Step 3: Send to server (SIO CONNECT):
        "40"          ← for namespace "/"
        "40/chat,"    ← for namespace "/chat"

Step 4: Receive from server (SIO CONNECT ack):
        "40"          ← connected!

Step 5: Ongoing — handle these frame types:
        Receive "2"  → send "3" (EIO ping/pong heartbeat — REQUIRED)
        Receive "42[event,data]" → your event arrived
        Send "42[event,data]"   → emit an event

Packet format:
        "4"  = EIO message prefix  (always present)
        "2"  = SIO EVENT type      (always for emitting events)
        So:  "42" + JSON array = emit event
        "42" + '["chat","hello"]' = emit "chat" event with arg "hello"
```

---

## Reconnection behaviour

All official socket.io client libraries reconnect automatically when the connection drops. Your gsocketio server handles reconnected clients the same as new connections — `OnConnect` fires again, and the client gets a fresh session ID.

For custom reconnect logic (raw WS clients):

```python
import websocket
import time

def connect_with_retry(url):
    while True:
        try:
            ws = websocket.create_connection(url)
            # ... use ws ...
        except Exception as e:
            print(f"Disconnected: {e} — retrying in 2s")
            time.sleep(2)
```

---

## Server-side Go code (your backend)

```go
package main

import (
    "encoding/json"
    "log"
    "net/http"

    sio "github.com/shishir1290/gsocketio"
)

type ChatMsg struct {
    Room string `json:"room"`
    User string `json:"user"`
    Text string `json:"text"`
}

func main() {
    srv := sio.New(nil)

    srv.OnConnect("/", func(c sio.Conn) error {
        log.Printf("connected: %s from %s", c.ID(), "")
        c.Join("general")
        c.Emit("welcome", "Connected to gsocketio!")
        return nil
    })

    srv.OnEvent("/", "chat", func(c sio.Conn, args []json.RawMessage) {
        var msg ChatMsg
        json.Unmarshal(args[0], &msg)
        // Broadcast to room, skip sender
        srv.ToRoom("/", msg.Room, "chat", c, msg)
    })

    srv.OnEvent("/", "join", func(c sio.Conn, args []json.RawMessage) {
        var room string
        json.Unmarshal(args[0], &room)
        c.Join(room)
        c.Emit("joined", room)
    })

    srv.OnDisconnect("/", func(c sio.Conn, reason string) {
        log.Printf("disconnected: %s (%s)", c.ID(), reason)
    })

    go srv.Serve()
    http.Handle("/socket.io/", srv)
    log.Fatal(http.ListenAndServe(":8080", nil))
}
```
