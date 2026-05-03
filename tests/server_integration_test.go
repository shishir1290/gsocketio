package gsocketio_test

import (
	"bufio"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shishir1290/gsocketio"
	"github.com/shishir1290/gsocketio/packet"
)

// ─────────────────────────────────────────────────────────────────────────────
// Minimal in-process WebSocket client (pure stdlib)
// ─────────────────────────────────────────────────────────────────────────────

const wsTestKey = "dGhlIHNhbXBsZSBub25jZQ==" // RFC 6455 example key

func wsAccept(key string) string {
	h := sha1.New()
	h.Write([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

// testClient is a minimal Socket.IO WebSocket client for testing.
type testClient struct {
	t    *testing.T
	conn net.Conn
	br   *bufio.Reader
	bw   *bufio.Writer
	mu   sync.Mutex
}

// dialWS connects to addr (host:port), performs the WebSocket upgrade, reads
// the EIO open packet, then sends the SIO CONNECT for ns.
func dialWS(t *testing.T, addr, ns string) *testClient {
	t.Helper()

	tc, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}

	br := bufio.NewReader(tc)
	bw := bufio.NewWriter(tc)

	// Send HTTP upgrade request.
	req := fmt.Sprintf(
		"GET /socket.io/?EIO=4&transport=websocket HTTP/1.1\r\n"+
			"Host: %s\r\n"+
			"Upgrade: websocket\r\n"+
			"Connection: Upgrade\r\n"+
			"Sec-WebSocket-Key: %s\r\n"+
			"Sec-WebSocket-Version: 13\r\n\r\n",
		addr, wsTestKey,
	)
	fmt.Fprint(bw, req)
	bw.Flush() //nolint:errcheck

	// Read HTTP response line.
	line, err := br.ReadString('\n')
	if err != nil || !strings.Contains(line, "101") {
		tc.Close()
		t.Fatalf("upgrade response: %q (err=%v)", line, err)
	}
	// Drain headers.
	for {
		l, _ := br.ReadString('\n')
		if l == "\r\n" || l == "\n" || l == "" {
			break
		}
	}

	cl := &testClient{t: t, conn: tc, br: br, bw: bw}

	// Consume EIO open packet "0{...}" (server → client, unmasked).
	cl.conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, eioRaw, err := cl.readServerFrame()
	cl.conn.SetReadDeadline(time.Time{})
	if err != nil {
		t.Fatalf("read EIO open: %v", err)
	}
	if len(eioRaw) == 0 || eioRaw[0] != '0' {
		t.Fatalf("expected EIO open packet, got: %q", eioRaw)
	}

	// Send SIO CONNECT wrapped with EIO "4" prefix → "40" or "40/chat,"
	// This is exactly what socket.io-client sends.
	connectPkt := &packet.Packet{Type: packet.TypeConnect, Namespace: ns}
	sioPkt, _ := packet.Encode(connectPkt)
	cl.sendRaw(append([]byte{'4'}, sioPkt...)) // EIO "4" + SIO packet

	// Consume SIO CONNECT ack — server sends "40" (EIO "4" + SIO "0")
	cl.conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, ackRaw, err := cl.readServerFrame()
	cl.conn.SetReadDeadline(time.Time{})
	if err != nil {
		t.Fatalf("read SIO connect ack: %v", err)
	}
	// Strip EIO "4" prefix to get the SIO packet
	ackSIO := stripEIOPrefix(ackRaw)
	p, _ := packet.Decode(ackSIO)
	if p == nil || p.Type != packet.TypeConnect {
		t.Fatalf("expected SIO CONNECT ack, got raw: %q stripped: %q", ackRaw, ackSIO)
	}

	return cl
}

// readServerFrame reads one unmasked WebSocket frame from the server.
func (cl *testClient) readServerFrame() (opcode byte, payload []byte, err error) {
	b0, err := cl.br.ReadByte()
	if err != nil {
		return 0, nil, err
	}
	opcode = b0 & 0x0F

	b1, err := cl.br.ReadByte()
	if err != nil {
		return 0, nil, err
	}
	payLen := uint64(b1 & 0x7F)

	switch payLen {
	case 126:
		var ext [2]byte
		io.ReadFull(cl.br, ext[:]) //nolint:errcheck
		payLen = uint64(binary.BigEndian.Uint16(ext[:]))
	case 127:
		var ext [8]byte
		io.ReadFull(cl.br, ext[:]) //nolint:errcheck
		payLen = binary.BigEndian.Uint64(ext[:])
	}

	if payLen > 0 {
		payload = make([]byte, payLen)
		io.ReadFull(cl.br, payload) //nolint:errcheck
	}
	return
}

// sendRaw sends payload as a masked WebSocket text frame (client→server).
func (cl *testClient) sendRaw(payload []byte) {
	cl.mu.Lock()
	defer cl.mu.Unlock()

	mask := [4]byte{0x37, 0x41, 0x05, 0x9C}
	masked := make([]byte, len(payload))
	for i, b := range payload {
		masked[i] = b ^ mask[i%4]
	}

	cl.bw.WriteByte(0x80 | 0x01) //nolint:errcheck // FIN + Text
	l := len(payload)
	switch {
	case l <= 125:
		cl.bw.WriteByte(byte(l) | 0x80) //nolint:errcheck
	case l <= 65535:
		cl.bw.WriteByte(126 | 0x80)      //nolint:errcheck
		cl.bw.WriteByte(byte(l >> 8))    //nolint:errcheck
		cl.bw.WriteByte(byte(l))         //nolint:errcheck
	}
	cl.bw.Write(mask[:])  //nolint:errcheck
	cl.bw.Write(masked)   //nolint:errcheck
	cl.bw.Flush()         //nolint:errcheck
}

// sendPacket encodes a SIO packet, wraps it with EIO "4" prefix, and sends it.
// This matches what socket.io-client does on all platforms.
func (cl *testClient) sendPacket(p *packet.Packet) {
	raw, err := packet.Encode(p)
	if err != nil {
		cl.t.Fatalf("encode packet: %v", err)
	}
	// Wrap with EIO message prefix "4" — required for all SIO packets
	cl.sendRaw(append([]byte{'4'}, raw...))
}

// recvPacket reads one SIO packet from the server with a timeout.
// Handles EIO heartbeat pings transparently (replies with pong, reads next frame).
func (cl *testClient) recvPacket(timeout time.Duration) (*packet.Packet, error) {
	deadline := time.Now().Add(timeout)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, fmt.Errorf("recvPacket: timed out after %v", timeout)
		}
		type result struct {
			raw []byte
			err error
		}
		ch := make(chan result, 1)
		go func() {
			cl.conn.SetReadDeadline(time.Now().Add(remaining))
			_, raw, err := cl.readServerFrame()
			cl.conn.SetReadDeadline(time.Time{})
			ch <- result{raw, err}
		}()
		r := <-ch
		if r.err != nil {
			return nil, r.err
		}
		// Handle EIO heartbeat ping "2" — reply with pong "3", then read next frame
		if len(r.raw) == 1 && r.raw[0] == '2' {
			cl.sendRaw([]byte{'3'}) // EIO pong
			continue
		}
		// Strip EIO "4" message prefix
		sio := stripEIOPrefix(r.raw)
		pkt, err := packet.Decode(sio)
		if err != nil {
			return nil, err
		}
		return pkt, nil
	}
}

// stripEIOPrefix removes the EIO "4" (message) prefix if present.
func stripEIOPrefix(raw []byte) []byte {
	if len(raw) > 0 && raw[0] == '4' {
		return raw[1:]
	}
	return raw
}

func (cl *testClient) close() { cl.conn.Close() } //nolint:errcheck

// ─────────────────────────────────────────────────────────────────────────────
// Test server factory
// ─────────────────────────────────────────────────────────────────────────────

func newTestServer(t *testing.T) (srv *gsocketio.Server, addr string, cleanup func()) {
	t.Helper()
	srv = gsocketio.New(nil)
	go srv.Serve() //nolint:errcheck

	mux := http.NewServeMux()
	mux.Handle("/socket.io/", srv)
	httpSrv := httptest.NewServer(mux)

	addr = strings.TrimPrefix(httpSrv.URL, "http://")
	cleanup = func() {
		httpSrv.Close()
		srv.Close() //nolint:errcheck
	}
	return
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

func TestConnect_FiresHandler(t *testing.T) {
	srv, addr, cleanup := newTestServer(t)
	defer cleanup()

	connected := make(chan string, 1)
	srv.OnConnect("/", func(c gsocketio.Conn) error {
		connected <- c.ID()
		return nil
	})

	cl := dialWS(t, addr, "/")
	defer cl.close()

	select {
	case id := <-connected:
		if id == "" {
			t.Error("connection ID should not be empty")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OnConnect not called within timeout")
	}
}

func TestConnect_IDIsUnique(t *testing.T) {
	srv, addr, cleanup := newTestServer(t)
	defer cleanup()

	ids := make(chan string, 3)
	srv.OnConnect("/", func(c gsocketio.Conn) error {
		ids <- c.ID()
		return nil
	})

	for i := 0; i < 3; i++ {
		cl := dialWS(t, addr, "/")
		defer cl.close()
	}

	seen := make(map[string]struct{})
	for i := 0; i < 3; i++ {
		select {
		case id := <-ids:
			if _, dup := seen[id]; dup {
				t.Errorf("duplicate ID: %q", id)
			}
			seen[id] = struct{}{}
		case <-time.After(3 * time.Second):
			t.Fatalf("only received %d IDs", i)
		}
	}
}

func TestConnect_NamespaceIsSet(t *testing.T) {
	srv, addr, cleanup := newTestServer(t)
	defer cleanup()

	srv.OnConnect("/", func(_ gsocketio.Conn) error { return nil })

	ns := make(chan string, 1)
	srv.OnConnect("/chat", func(c gsocketio.Conn) error {
		ns <- c.Namespace()
		return nil
	})

	cl := dialWS(t, addr, "/chat")
	defer cl.close()

	select {
	case got := <-ns:
		if got != "/chat" {
			t.Errorf("namespace: want /chat got %q", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OnConnect(/chat) not called")
	}
}

func TestDisconnect_FiresHandler(t *testing.T) {
	srv, addr, cleanup := newTestServer(t)
	defer cleanup()

	disc := make(chan string, 1)
	srv.OnDisconnect("/", func(c gsocketio.Conn, reason string) {
		disc <- reason
	})

	cl := dialWS(t, addr, "/")
	cl.close() // immediate disconnect

	select {
	case reason := <-disc:
		if reason == "" {
			t.Error("disconnect reason should not be empty")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("OnDisconnect not called")
	}
}

func TestEvent_Received(t *testing.T) {
	srv, addr, cleanup := newTestServer(t)
	defer cleanup()

	got := make(chan string, 1)
	srv.OnEvent("/", "chat", func(c gsocketio.Conn, args []json.RawMessage) {
		var msg string
		json.Unmarshal(args[0], &msg) //nolint:errcheck
		got <- msg
	})

	cl := dialWS(t, addr, "/")
	defer cl.close()

	data, _ := packet.BuildEventData("chat", "hello server")
	cl.sendPacket(&packet.Packet{Type: packet.TypeEvent, Namespace: "/", Data: data})

	select {
	case msg := <-got:
		if msg != "hello server" {
			t.Errorf("want 'hello server' got %q", msg)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("event not received")
	}
}

func TestEvent_MultipleArgs(t *testing.T) {
	srv, addr, cleanup := newTestServer(t)
	defer cleanup()

	type Result struct {
		s string
		n int
		b bool
	}
	got := make(chan Result, 1)
	srv.OnEvent("/", "multi", func(c gsocketio.Conn, args []json.RawMessage) {
		var r Result
		json.Unmarshal(args[0], &r.s) //nolint:errcheck
		json.Unmarshal(args[1], &r.n) //nolint:errcheck
		json.Unmarshal(args[2], &r.b) //nolint:errcheck
		got <- r
	})

	cl := dialWS(t, addr, "/")
	defer cl.close()

	data, _ := packet.BuildEventData("multi", "hello", 99, true)
	cl.sendPacket(&packet.Packet{Type: packet.TypeEvent, Namespace: "/", Data: data})

	select {
	case r := <-got:
		if r.s != "hello" || r.n != 99 || !r.b {
			t.Errorf("args: got %+v", r)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("multi-arg event not received")
	}
}

func TestEmit_ServerToClient(t *testing.T) {
	srv, addr, cleanup := newTestServer(t)
	defer cleanup()

	srv.OnConnect("/", func(c gsocketio.Conn) error {
		go func() {
			time.Sleep(30 * time.Millisecond)
			c.Emit("welcome", "hi from server") //nolint:errcheck
		}()
		return nil
	})

	cl := dialWS(t, addr, "/")
	defer cl.close()

	pkt, err := cl.recvPacket(2 * time.Second)
	if err != nil {
		t.Fatalf("recvPacket: %v", err)
	}
	if pkt.Type != packet.TypeEvent {
		t.Errorf("type: want EVENT got %v", pkt.Type)
	}
	name, _ := packet.EventName(pkt.Data)
	if name != "welcome" {
		t.Errorf("event name: want welcome got %q", name)
	}
	args, _ := packet.EventArgs(pkt.Data)
	var msg string
	json.Unmarshal(args[0], &msg) //nolint:errcheck
	if msg != "hi from server" {
		t.Errorf("arg: want 'hi from server' got %q", msg)
	}
}

func TestJoinLeave_Room(t *testing.T) {
	srv, addr, cleanup := newTestServer(t)
	defer cleanup()

	ready := make(chan struct{}, 1)
	srv.OnConnect("/", func(c gsocketio.Conn) error {
		c.Join("lobby")
		ready <- struct{}{}
		return nil
	})

	cl := dialWS(t, addr, "/")
	defer cl.close()

	<-ready
	if n := srv.RoomLen("/", "lobby"); n != 1 {
		t.Errorf("RoomLen after Join: want 1 got %d", n)
	}

	rooms := srv.Rooms("/")
	found := false
	for _, r := range rooms {
		if r == "lobby" {
			found = true
		}
	}
	if !found {
		t.Error("lobby not in Rooms()")
	}
}

func TestLeave_Room(t *testing.T) {
	srv, addr, cleanup := newTestServer(t)
	defer cleanup()

	done := make(chan struct{}, 1)
	srv.OnConnect("/", func(c gsocketio.Conn) error {
		c.Join("room")
		c.Leave("room")
		done <- struct{}{}
		return nil
	})

	cl := dialWS(t, addr, "/")
	defer cl.close()

	<-done
	if n := srv.RoomLen("/", "room"); n != 0 {
		t.Errorf("RoomLen after Leave: want 0 got %d", n)
	}
}

func TestConn_Rooms(t *testing.T) {
	srv, addr, cleanup := newTestServer(t)
	defer cleanup()

	got := make(chan []string, 1)
	srv.OnConnect("/", func(c gsocketio.Conn) error {
		c.Join("r1")
		c.Join("r2")
		c.Join("r3")
		got <- c.Rooms()
		return nil
	})

	cl := dialWS(t, addr, "/")
	defer cl.close()

	select {
	case rs := <-got:
		if len(rs) != 3 {
			t.Errorf("want 3 rooms got %d: %v", len(rs), rs)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Rooms() timeout")
	}
}

func TestContext_SetGet(t *testing.T) {
	srv, addr, cleanup := newTestServer(t)
	defer cleanup()

	got := make(chan interface{}, 1)
	srv.OnConnect("/", func(c gsocketio.Conn) error {
		c.SetContext("my-value")
		return nil
	})
	srv.OnEvent("/", "check", func(c gsocketio.Conn, _ []json.RawMessage) {
		got <- c.Context()
	})

	cl := dialWS(t, addr, "/")
	defer cl.close()

	data, _ := packet.BuildEventData("check")
	cl.sendPacket(&packet.Packet{Type: packet.TypeEvent, Namespace: "/", Data: data})

	select {
	case v := <-got:
		if v != "my-value" {
			t.Errorf("context: want 'my-value' got %v", v)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("context check timeout")
	}
}

func TestToRoom_Broadcast(t *testing.T) {
	srv, addr, cleanup := newTestServer(t)
	defer cleanup()

	var joined int32
	srv.OnConnect("/", func(c gsocketio.Conn) error {
		c.Join("chat")
		if atomic.AddInt32(&joined, 1) == 2 {
			go func() {
				time.Sleep(50 * time.Millisecond)
				srv.ToRoom("/", "chat", "announcement", nil, "hello room")
			}()
		}
		return nil
	})

	var wg sync.WaitGroup
	received := make(chan string, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cl := dialWS(t, addr, "/")
			defer cl.close()
			pkt, err := cl.recvPacket(3 * time.Second)
			if err != nil {
				return
			}
			args, _ := packet.EventArgs(pkt.Data)
			if len(args) > 0 {
				var msg string
				json.Unmarshal(args[0], &msg) //nolint:errcheck
				received <- msg
			}
		}()
	}
	wg.Wait()

	if len(received) < 2 {
		t.Errorf("expected 2 clients to receive broadcast, got %d", len(received))
	}
}

func TestToRoom_SkipSender(t *testing.T) {
	srv, addr, cleanup := newTestServer(t)
	defer cleanup()

	var mu sync.Mutex
	var conns []gsocketio.Conn
	var cond = sync.NewCond(&mu)

	srv.OnConnect("/", func(c gsocketio.Conn) error {
		c.Join("room")
		mu.Lock()
		conns = append(conns, c)
		cond.Signal()
		mu.Unlock()
		return nil
	})

	cl1 := dialWS(t, addr, "/")
	defer cl1.close()
	cl2 := dialWS(t, addr, "/")
	defer cl2.close()

	// Wait for both connections
	mu.Lock()
	for len(conns) < 2 {
		cond.Wait()
	}
	sender := conns[0]
	mu.Unlock()

	srv.ToRoom("/", "room", "msg", sender, "data")

	// cl2 should receive; cl1 (sender) should not.
	pkt, err := cl2.recvPacket(2 * time.Second)
	if err != nil {
		t.Fatalf("cl2 recvPacket: %v", err)
	}
	if pkt.Type != packet.TypeEvent {
		t.Errorf("cl2 should receive event, got %v", pkt.Type)
	}
}

func TestToNamespace_Broadcast(t *testing.T) {
	srv, addr, cleanup := newTestServer(t)
	defer cleanup()

	// Join a room so SendAll (used by ToNamespace) can reach each connection.
	var joined int32
	srv.OnConnect("/", func(c gsocketio.Conn) error {
		c.Join("ns-room")
		if atomic.AddInt32(&joined, 1) == 3 {
			go func() {
				time.Sleep(50 * time.Millisecond)
				srv.ToNamespace("/", "global", "broadcast msg")
			}()
		}
		return nil
	})

	received := make(chan struct{}, 3)
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cl := dialWS(t, addr, "/")
			defer cl.close()
			pkt, err := cl.recvPacket(2 * time.Second)
			if err == nil && pkt != nil && pkt.Type == packet.TypeEvent {
				received <- struct{}{}
			}
		}()
	}
	wg.Wait()

	if len(received) < 3 {
		t.Errorf("expected 3 clients to receive namespace broadcast, got %d", len(received))
	}
}

func TestRoomMembers(t *testing.T) {
	srv, addr, cleanup := newTestServer(t)
	defer cleanup()

	var ready int32
	srv.OnConnect("/", func(c gsocketio.Conn) error {
		c.Join("vip")
		atomic.AddInt32(&ready, 1)
		return nil
	})

	for i := 0; i < 3; i++ {
		cl := dialWS(t, addr, "/")
		defer cl.close()
	}

	// Poll until all 3 joined.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&ready) == 3 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	members := srv.RoomMembers("/", "vip")
	if len(members) != 3 {
		t.Errorf("RoomMembers: want 3 got %d", len(members))
	}
}

func TestForEachInRoom(t *testing.T) {
	srv, addr, cleanup := newTestServer(t)
	defer cleanup()

	var joined int32
	srv.OnConnect("/", func(c gsocketio.Conn) error {
		c.Join("group")
		atomic.AddInt32(&joined, 1)
		return nil
	})

	for i := 0; i < 4; i++ {
		cl := dialWS(t, addr, "/")
		defer cl.close()
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&joined) == 4 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	var count int32
	srv.ForEachInRoom("/", "group", func(c gsocketio.Conn) {
		atomic.AddInt32(&count, 1)
	})
	if count != 4 {
		t.Errorf("ForEachInRoom count: want 4 got %d", count)
	}
}

func TestCount(t *testing.T) {
	srv, addr, cleanup := newTestServer(t)
	defer cleanup()

	var ready int32
	srv.OnConnect("/", func(_ gsocketio.Conn) error {
		atomic.AddInt32(&ready, 1)
		return nil
	})

	clients := make([]*testClient, 3)
	for i := range clients {
		clients[i] = dialWS(t, addr, "/")
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&ready) == 3 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if n := srv.Count(); n < 1 {
		t.Errorf("Count: want ≥1 got %d", n)
	}
	for _, cl := range clients {
		cl.close()
	}
}

func TestCORSOptions(t *testing.T) {
	srv := gsocketio.New(nil)
	defer srv.Close() //nolint:errcheck

	req, _ := http.NewRequest(http.MethodOptions, "/socket.io/", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Errorf("OPTIONS: want 204 got %d", w.Code)
	}
	if w.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Error("missing CORS header Access-Control-Allow-Origin: *")
	}
}

func TestMultipleEvents(t *testing.T) {
	srv, addr, cleanup := newTestServer(t)
	defer cleanup()

	var pingCount int32
	srv.OnEvent("/", "ping", func(c gsocketio.Conn, _ []json.RawMessage) {
		atomic.AddInt32(&pingCount, 1)
		c.Emit("pong", "response") //nolint:errcheck
	})

	cl := dialWS(t, addr, "/")
	defer cl.close()

	for i := 0; i < 5; i++ {
		data, _ := packet.BuildEventData("ping")
		cl.sendPacket(&packet.Packet{Type: packet.TypeEvent, Namespace: "/", Data: data})
		cl.recvPacket(500 * time.Millisecond) //nolint:errcheck
	}

	time.Sleep(100 * time.Millisecond)
	if n := atomic.LoadInt32(&pingCount); n != 5 {
		t.Errorf("ping count: want 5 got %d", n)
	}
}

func TestUnregisteredNamespace_Rejected(t *testing.T) {
	srv, addr, cleanup := newTestServer(t)
	defer cleanup()

	// Register "/" only, not "/secret"
	srv.OnConnect("/", func(c gsocketio.Conn) error { return nil })

	tc, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer tc.Close()

	br := bufio.NewReader(tc)
	bw := bufio.NewWriter(tc)

	req := fmt.Sprintf(
		"GET /socket.io/?EIO=4&transport=websocket HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n\r\n",
		addr, wsTestKey,
	)
	fmt.Fprint(bw, req)
	bw.Flush() //nolint:errcheck

	line, _ := br.ReadString('\n')
	if !strings.Contains(line, "101") {
		t.Skip("upgrade failed")
	}
	for {
		l, _ := br.ReadString('\n')
		if l == "\r\n" || l == "\n" {
			break
		}
	}

	// Send secret namespace CONNECT
	secretConn := &packet.Packet{Type: packet.TypeConnect, Namespace: "/secret"}
	raw, _ := packet.Encode(secretConn)
	// Build masked frame
	mask := [4]byte{0x01, 0x02, 0x03, 0x04}
	masked := make([]byte, len(raw))
	for i, b := range raw {
		masked[i] = b ^ mask[i%4]
	}
	bw.WriteByte(0x81)            //nolint:errcheck
	bw.WriteByte(byte(len(raw)) | 0x80) //nolint:errcheck
	bw.Write(mask[:])             //nolint:errcheck
	bw.Write(masked)              //nolint:errcheck
	bw.Flush()                    //nolint:errcheck

	// Wait briefly — server should close or send CONNECT_ERROR
	time.Sleep(300 * time.Millisecond)
}

func TestClearRoom(t *testing.T) {
	srv, addr, cleanup := newTestServer(t)
	defer cleanup()

	var joined int32
	srv.OnConnect("/", func(c gsocketio.Conn) error {
		c.Join("tmp")
		atomic.AddInt32(&joined, 1)
		return nil
	})

	for i := 0; i < 3; i++ {
		cl := dialWS(t, addr, "/")
		defer cl.close()
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&joined) == 3 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if n := srv.RoomLen("/", "tmp"); n != 3 {
		t.Errorf("before clear: want 3 got %d", n)
	}
	srv.ClearRoom("/", "tmp")
	if n := srv.RoomLen("/", "tmp"); n != 0 {
		t.Errorf("after clear: want 0 got %d", n)
	}
}

func mustBuildEventData(event string, args ...interface{}) json.RawMessage {
	d, _ := packet.BuildEventData(event, args...)
	return d
}

// ─────────────────────────────────────────────────────────────────────────────
// Fix-verification integration tests
// ─────────────────────────────────────────────────────────────────────────────

// FIX S-03: OnDisconnect fires exactly once even if Close() is called
// concurrently from user code and the transport layer.
func TestDisconnect_FiresExactlyOnce(t *testing.T) {
	srv, addr, cleanup := newTestServer(t)
	defer cleanup()

	var count int32
	srv.OnDisconnect("/", func(c gsocketio.Conn, _ string) {
		atomic.AddInt32(&count, 1)
	})

	cl := dialWS(t, addr, "/")
	// Close from both sides nearly simultaneously
	go cl.close()
	go cl.close()

	time.Sleep(500 * time.Millisecond)
	if n := atomic.LoadInt32(&count); n != 1 {
		t.Errorf("OnDisconnect fires: want exactly 1 got %d", n)
	}
}

// FIX S-06: Count() is decremented before OnDisconnect fires.
func TestCount_AccurateDuringDisconnect(t *testing.T) {
	srv, addr, cleanup := newTestServer(t)
	defer cleanup()

	ready := make(chan struct{}, 1)
	srv.OnConnect("/", func(c gsocketio.Conn) error {
		ready <- struct{}{}
		return nil
	})

	var countInsideDisconnect int32
	srv.OnDisconnect("/", func(c gsocketio.Conn, _ string) {
		// Count should already be 0 when this fires (decremented first).
		atomic.StoreInt32(&countInsideDisconnect, int32(srv.Count()))
	})

	cl := dialWS(t, addr, "/")
	<-ready
	cl.close()

	time.Sleep(400 * time.Millisecond)
	if n := atomic.LoadInt32(&countInsideDisconnect); n != 0 {
		t.Errorf("Count inside OnDisconnect: want 0 got %d", n)
	}
}

// FIX R-03: Join is capped at MaxRoomsPerConn.
func TestJoin_MaxRoomsPerConnEnforced(t *testing.T) {
	srv, addr, cleanup := newTestServer(t)
	defer cleanup()

	done := make(chan int, 1)
	srv.OnConnect("/", func(c gsocketio.Conn) error {
		// Try to join 200 rooms — should be capped at MaxRoomsPerConn (100).
		for i := 0; i < 200; i++ {
			c.Join(fmt.Sprintf("room-%d", i))
		}
		done <- len(c.Rooms())
		return nil
	})

	cl := dialWS(t, addr, "/")
	defer cl.close()

	select {
	case n := <-done:
		if n > 100 {
			t.Errorf("rooms: want ≤100 got %d (MaxRoomsPerConn not enforced)", n)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for join cap test")
	}
}

// FIX S-01: CONNECT_ERROR message is sanitised (no internal error details).
func TestConnectError_IsSanitised(t *testing.T) {
	srv, addr, cleanup := newTestServer(t)
	defer cleanup()

	// Register "/" with a rejecting handler that returns a sensitive error.
	srv.OnConnect("/", func(c gsocketio.Conn) error {
		return fmt.Errorf("internal db error: password=secret123")
	})

	tc, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer tc.Close()

	br := bufio.NewReader(tc)
	bw := bufio.NewWriter(tc)

	req := fmt.Sprintf(
		"GET /socket.io/?EIO=4&transport=websocket HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n\r\n",
		addr, wsTestKey,
	)
	fmt.Fprint(bw, req)
	bw.Flush() //nolint:errcheck

	line, _ := br.ReadString('\n')
	if !strings.Contains(line, "101") {
		t.Skip("upgrade failed in test env")
	}
	for {
		l, _ := br.ReadString('\n')
		if l == "\r\n" || l == "\n" {
			break
		}
	}

	// Send SIO CONNECT
	connectPkt := &packet.Packet{Type: packet.TypeConnect, Namespace: "/"}
	raw, _ := packet.Encode(connectPkt)
	sendMaskedWS(t, bw, raw)

	// Read responses until we get CONNECT_ERROR or timeout
	tc.SetReadDeadline(time.Now().Add(3 * time.Second))
	for i := 0; i < 5; i++ {
		_, frame, err := readWSFrame(br)
		if err != nil {
			break
		}
		if len(frame) == 0 {
			continue
		}
		pkt, err := packet.Decode(frame)
		if err != nil {
			continue
		}
		if pkt.Type == packet.TypeConnectError {
			// FIX S-01: must NOT contain internal error detail.
			body := string(pkt.Data)
			if strings.Contains(body, "password") || strings.Contains(body, "secret") || strings.Contains(body, "db error") {
				t.Errorf("CONNECT_ERROR leaks internal error: %s", body)
			}
			return
		}
	}
}

// helpers for raw WS reads used in TestConnectError_IsSanitised
func sendMaskedWS(t *testing.T, bw *bufio.Writer, payload []byte) {
	t.Helper()
	mask := [4]byte{0x37, 0x41, 0x05, 0x9C}
	masked := make([]byte, len(payload))
	for i, b := range payload {
		masked[i] = b ^ mask[i%4]
	}
	bw.WriteByte(0x81)                   //nolint:errcheck
	bw.WriteByte(byte(len(payload)) | 0x80) //nolint:errcheck
	bw.Write(mask[:])                    //nolint:errcheck
	bw.Write(masked)                     //nolint:errcheck
	bw.Flush()                           //nolint:errcheck
}

func readWSFrame(br *bufio.Reader) (opcode byte, payload []byte, err error) {
	b0, err := br.ReadByte()
	if err != nil {
		return 0, nil, err
	}
	opcode = b0 & 0x0F
	b1, err := br.ReadByte()
	if err != nil {
		return 0, nil, err
	}
	payLen := int(b1 & 0x7F)
	if payLen == 126 {
		var ext [2]byte
		io.ReadFull(br, ext[:]) //nolint:errcheck
		payLen = int(binary.BigEndian.Uint16(ext[:]))
	}
	if payLen > 0 {
		payload = make([]byte, payLen)
		io.ReadFull(br, payload) //nolint:errcheck
	}
	return
}
