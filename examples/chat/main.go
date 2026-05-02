// Chat example — a working multi-room chat server using gsocketio.
//
// Usage:
//
//	go run examples/chat/main.go
//
// Then open http://localhost:8080 in two browser tabs and chat.
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/shishir1290/gsocketio"
	"github.com/shishir1290/gsocketio/logger"
)

// chatMessage is the JSON body clients exchange.
type chatMessage struct {
	Room string `json:"room"`
	Text string `json:"text"`
	User string `json:"user"`
}

const html = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>gsocketio Chat</title>
<style>
  body { font-family: Arial, sans-serif; max-width: 700px; margin: 40px auto; }
  #log  { height: 320px; overflow-y: auto; border: 1px solid #ccc;
          padding: 8px; border-radius: 4px; background: #fafafa; }
  .msg  { margin: 4px 0; }
  .sys  { color: #888; font-style: italic; }
  input { width: 480px; padding: 6px; }
  button{ padding: 6px 14px; }
</style>
</head>
<body>
<h2>gsocketio Chat Demo</h2>
<div id="log"></div>
<br>
<label>Room: <input id="room" value="general" style="width:120px"></label>
<label>Name: <input id="name" value="Guest" style="width:100px"></label>
<br><br>
<input id="msg" placeholder="Type a message and press Enter…">
<button onclick="sendMsg()">Send</button>
<br><br>
<button onclick="joinRoom()">Join Room</button>
<button onclick="leaveRoom()">Leave Room</button>

<script>
const log = document.getElementById('log');
const append = (txt, cls='msg') => {
  const d = document.createElement('div');
  d.className = cls;
  d.textContent = txt;
  log.appendChild(d);
  log.scrollTop = log.scrollHeight;
};

// Connect via WebSocket (Socket.IO wire protocol implemented manually).
const ws = new WebSocket('ws://' + location.host + '/socket.io/?EIO=4&transport=websocket');
let sioReady = false;

ws.onopen = () => append('[system] WebSocket open', 'sys');

ws.onmessage = e => {
  const raw = e.data;
  const type = raw[0];

  if (type === '0') {               // EIO OPEN
    ws.send('0');                   // SIO CONNECT to "/"
    return;
  }
  if (type === '1') {               // EIO CLOSE
    append('[system] server closed connection', 'sys');
    return;
  }
  if (raw === '0') { sioReady = true; append('[system] connected ✓', 'sys'); return; }

  if (type === '2') {               // SIO EVENT
    try {
      const arr = JSON.parse(raw.slice(1));
      const [event, ...args] = arr;
      if (event === 'chat') {
        const m = args[0];
        append('[' + m.room + '] ' + m.user + ': ' + m.text);
      } else if (event === 'system') {
        append('[system] ' + args[0], 'sys');
      }
    } catch(err) { console.error(err); }
  }
};

ws.onclose = () => append('[system] disconnected', 'sys');

function emit(event, ...args) {
  ws.send('2' + JSON.stringify([event, ...args]));
}

function sendMsg() {
  const text = document.getElementById('msg').value.trim();
  if (!text || !sioReady) return;
  emit('chat', {
    room: document.getElementById('room').value || 'general',
    text,
    user: document.getElementById('name').value || 'Guest',
  });
  document.getElementById('msg').value = '';
}

function joinRoom() {
  const room = document.getElementById('room').value || 'general';
  emit('join', room);
}

function leaveRoom() {
  const room = document.getElementById('room').value || 'general';
  emit('leave', room);
}

document.getElementById('msg').addEventListener('keydown', e => {
  if (e.key === 'Enter') sendMsg();
});
</script>
</body>
</html>`

func main() {
	logger.SetLevel(logger.LevelInfo)

	srv := gsocketio.New(nil)

	// ── connection lifecycle ──────────────────────────────────────────────────

	srv.OnConnect("/", func(c gsocketio.Conn) error {
		log.Printf("connect   sid=%s addr=%s", c.ID(), "")
		c.Join("general") // everyone starts in the general room
		c.SetContext(map[string]string{"name": "Guest"})
		c.Emit("system", fmt.Sprintf("Welcome! You are connected as %s", c.ID())) //nolint:errcheck
		return nil
	})

	srv.OnDisconnect("/", func(c gsocketio.Conn, reason string) {
		log.Printf("disconnect sid=%s reason=%s", c.ID(), reason)
	})

	srv.OnError("/", func(c gsocketio.Conn, err error) {
		log.Printf("error sid=%s: %v", c.ID(), err)
	})

	// ── events ────────────────────────────────────────────────────────────────

	// chat — broadcast to everyone in the room, skip the sender.
	srv.OnEvent("/", "chat", func(c gsocketio.Conn, args []json.RawMessage) {
		if len(args) == 0 {
			return
		}
		var msg chatMessage
		if err := json.Unmarshal(args[0], &msg); err != nil {
			return
		}
		if msg.Room == "" {
			msg.Room = "general"
		}
		log.Printf("chat room=%s user=%s text=%s", msg.Room, msg.User, msg.Text)
		srv.ToRoom("/", msg.Room, "chat", c, msg)
	})

	// join — add the connection to a named room.
	srv.OnEvent("/", "join", func(c gsocketio.Conn, args []json.RawMessage) {
		if len(args) == 0 {
			return
		}
		var room string
		if err := json.Unmarshal(args[0], &room); err != nil || room == "" {
			return
		}
		c.Join(room)
		log.Printf("join  sid=%s room=%s", c.ID(), room)
		c.Emit("system", fmt.Sprintf("You joined #%s  (members: %d)", room, srv.RoomLen("/", room))) //nolint:errcheck
	})

	// leave — remove the connection from a named room.
	srv.OnEvent("/", "leave", func(c gsocketio.Conn, args []json.RawMessage) {
		if len(args) == 0 {
			return
		}
		var room string
		if err := json.Unmarshal(args[0], &room); err != nil || room == "" {
			return
		}
		c.Leave(room)
		log.Printf("leave sid=%s room=%s", c.ID(), room)
		c.Emit("system", fmt.Sprintf("You left #%s", room)) //nolint:errcheck
	})

	// ── HTTP mux ──────────────────────────────────────────────────────────────

	go srv.Serve() //nolint:errcheck

	mux := http.NewServeMux()
	mux.Handle("/socket.io/", srv)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		fmt.Fprint(w, html)
	})

	log.Println("gsocketio chat server listening on http://localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", mux))
}
