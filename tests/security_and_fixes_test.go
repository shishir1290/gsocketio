package gsocketio_test

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shishir1290/gsocketio"
	"github.com/shishir1290/gsocketio/packet"
)

// 1. Critical: Acked events run the handler exactly once.
func TestAck_HandlerExecutedOnlyOnce(t *testing.T) {
	srv := gsocketio.New(nil)
	var execCount int32

	srv.OnEvent("/", "order", func(c gsocketio.Conn, args []json.RawMessage) {
		atomic.AddInt32(&execCount, 1)
	})

	ts := httptest.NewServer(http.HandlerFunc(srv.ServeHTTP))
	defer ts.Close()
	defer srv.Close()

	go srv.Serve() //nolint:errcheck

	cl := dialWS(t, ts.Listener.Addr().String(), "/")
	defer cl.conn.Close()

	// Send an event with an ack ID: "420[\"order\",\"item1\"]"
	// SIO Type 2 (Event), ID 0
	pkt := &packet.Packet{
		Type:      packet.TypeEvent,
		Namespace: "/",
		ID:        intPtr(0),
		Data:      json.RawMessage(`["order","item1"]`),
	}
	sioPkt, _ := packet.Encode(pkt)
	cl.sendRaw(append([]byte{'4'}, sioPkt...))

	// Read server response; should receive SIO Type 3 (Ack), ID 0: "430"
	cl.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, raw, err := cl.readServerFrame()
	if err != nil {
		t.Fatalf("read ack frame: %v", err)
	}

	ackPkt, err := packet.Decode(stripEIOPrefix(raw))
	if err != nil || ackPkt == nil || ackPkt.Type != packet.TypeAck || ackPkt.ID == nil || *ackPkt.ID != 0 {
		t.Fatalf("expected TypeAck with ID 0, got: %q", raw)
	}

	time.Sleep(50 * time.Millisecond)
	got := atomic.LoadInt32(&execCount)
	if got != 1 {
		t.Fatalf("handler executed %d times, expected exactly 1", got)
	}
}

// 2. Critical: Origin validation / CORS checks.
func TestOrigin_Validation(t *testing.T) {
	opts := &gsocketio.Options{
		AllowedOrigins: []string{"http://trusted.com", "https://app.trusted.com"},
	}
	srv := gsocketio.New(opts)
	ts := httptest.NewServer(http.HandlerFunc(srv.ServeHTTP))
	defer ts.Close()
	defer srv.Close()

	go srv.Serve() //nolint:errcheck

	// Disallowed origin on polling GET
	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/socket.io/?EIO=4&transport=polling", nil)
	req.Header.Set("Origin", "http://evil.com")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET evil origin: %v", err)
	}
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden for evil origin, got: %d", resp.StatusCode)
	}
	resp.Body.Close()

	// Allowed origin on polling GET
	req, _ = http.NewRequest(http.MethodGet, ts.URL+"/socket.io/?EIO=4&transport=polling", nil)
	req.Header.Set("Origin", "http://trusted.com")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET trusted origin: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for trusted origin, got: %d", resp.StatusCode)
	}
	if resp.Header.Get("Access-Control-Allow-Origin") != "http://trusted.com" {
		t.Fatalf("expected Access-Control-Allow-Origin header set, got: %q", resp.Header.Get("Access-Control-Allow-Origin"))
	}
	resp.Body.Close()

	// Disallowed origin on WebSocket upgrade
	tc, err := net.Dial("tcp", ts.Listener.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer tc.Close()

	reqStr := fmt.Sprintf(
		"GET /socket.io/?EIO=4&transport=websocket HTTP/1.1\r\n"+
			"Host: %s\r\n"+
			"Origin: http://evil.com\r\n"+
			"Upgrade: websocket\r\n"+
			"Connection: Upgrade\r\n"+
			"Sec-WebSocket-Key: %s\r\n"+
			"Sec-WebSocket-Version: 13\r\n\r\n",
		ts.Listener.Addr().String(), wsTestKey,
	)
	fmt.Fprint(tc, reqStr)

	br := bufio.NewReader(tc)
	line, _ := br.ReadString('\n')
	if !strings.Contains(line, "403") {
		t.Fatalf("expected 403 for evil WS origin, got: %q", line)
	}
}

func TestOrigin_CustomCheckOrigin(t *testing.T) {
	opts := &gsocketio.Options{
		CheckOrigin: func(r *http.Request) bool {
			return r.Header.Get("X-Secret-Token") == "allow-me"
		},
	}
	srv := gsocketio.New(opts)
	ts := httptest.NewServer(http.HandlerFunc(srv.ServeHTTP))
	defer ts.Close()
	defer srv.Close()

	// Without token -> 403
	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/socket.io/?EIO=4&transport=polling", nil)
	resp, _ := http.DefaultClient.Do(req)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 without token, got: %d", resp.StatusCode)
	}
	resp.Body.Close()

	// With token -> 200
	req, _ = http.NewRequest(http.MethodGet, ts.URL+"/socket.io/?EIO=4&transport=polling", nil)
	req.Header.Set("X-Secret-Token", "allow-me")
	resp, _ = http.DefaultClient.Do(req)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 with valid token, got: %d", resp.StatusCode)
	}
	resp.Body.Close()
}

// 3. High: Slow polling client doesn't hang room broadcast.
func TestSlowPoll_WriteTimeoutDoesNotBlockBroadcast(t *testing.T) {
	srv := gsocketio.New(nil)
	var connReady = make(chan struct{})

	srv.OnConnect("/", func(c gsocketio.Conn) error {
		c.Join("broadcast-room")
		close(connReady)
		return nil
	})

	ts := httptest.NewServer(http.HandlerFunc(srv.ServeHTTP))
	defer ts.Close()
	defer srv.Close()

	go srv.Serve() //nolint:errcheck

	// Start polling session (bare GET)
	resp, err := http.Get(ts.URL + "/socket.io/?EIO=4&transport=polling")
	if err != nil {
		t.Fatalf("bare GET: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	var op openPacketTest
	_ = json.Unmarshal(body[1:], &op)
	sid := op.SID

	// Send connect packet via POST: "40"
	postURL := fmt.Sprintf("%s/socket.io/?EIO=4&transport=polling&sid=%s", ts.URL, sid)
	postResp, err := http.Post(postURL, "text/plain", strings.NewReader("40"))
	if err != nil {
		t.Fatalf("POST connect: %v", err)
	}
	postResp.Body.Close()

	select {
	case <-connReady:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for connection")
	}

	// Now broadcast 300 times without the client performing any GET requests.
	// Since the send channel is 256 slots, subsequent sends should not block indefinitely.
	done := make(chan struct{})
	go func() {
		for i := 0; i < 300; i++ {
			srv.ToRoom("/", "broadcast-room", "news", nil, fmt.Sprintf("msg-%d", i))
		}
		close(done)
	}()

	select {
	case <-done:
		// Broadcasts completed without hanging
	case <-time.After(3 * time.Second):
		t.Fatal("room broadcast hung due to slow polling client")
	}
}

type openPacketTest struct {
	SID string `json:"sid"`
}

// 4. High: Connection/resource caps.
func TestConnectionCaps_MaxConnections(t *testing.T) {
	opts := &gsocketio.Options{
		MaxConnections: 2,
	}
	srv := gsocketio.New(opts)
	ts := httptest.NewServer(http.HandlerFunc(srv.ServeHTTP))
	defer ts.Close()
	defer srv.Close()

	go srv.Serve() //nolint:errcheck

	// Connect 1
	r1, _ := http.Get(ts.URL + "/socket.io/?EIO=4&transport=polling")
	if r1.StatusCode != http.StatusOK {
		t.Fatalf("conn 1 want 200 got %d", r1.StatusCode)
	}
	b1, _ := io.ReadAll(r1.Body)
	r1.Body.Close()
	var op1 openPacketTest
	json.Unmarshal(b1[1:], &op1)

	// Connect 2
	r2, _ := http.Get(ts.URL + "/socket.io/?EIO=4&transport=polling")
	if r2.StatusCode != http.StatusOK {
		t.Fatalf("conn 2 want 200 got %d", r2.StatusCode)
	}
	b2, _ := io.ReadAll(r2.Body)
	r2.Body.Close()
	var op2 openPacketTest
	json.Unmarshal(b2[1:], &op2)

	// Connect 3 (should be rejected with 503)
	r3, _ := http.Get(ts.URL + "/socket.io/?EIO=4&transport=polling")
	if r3.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("conn 3 want 503 Service Unavailable, got %d", r3.StatusCode)
	}
	r3.Body.Close()

	// Close conn 1 and verify a new connection can now connect
	postURL := fmt.Sprintf("%s/socket.io/?EIO=4&transport=polling&sid=%s", ts.URL, op1.SID)
	closeReq, _ := http.Post(postURL, "text/plain", strings.NewReader("1")) // EIO 1 = close
	if closeReq != nil {
		closeReq.Body.Close()
	}
	time.Sleep(50 * time.Millisecond)

	r4, _ := http.Get(ts.URL + "/socket.io/?EIO=4&transport=polling")
	if r4.StatusCode != http.StatusOK {
		t.Fatalf("conn 4 after freeing slot want 200, got %d", r4.StatusCode)
	}
	r4.Body.Close()
}

// 5. Medium: Server.JoinRoom enforces per-connection room cap and syncs conn.Rooms().
func TestServer_JoinRoom_CapAndSync(t *testing.T) {
	srv := gsocketio.New(nil)
	var conn gsocketio.Conn
	connReady := make(chan struct{})

	srv.OnConnect("/", func(c gsocketio.Conn) error {
		conn = c
		close(connReady)
		return nil
	})

	ts := httptest.NewServer(http.HandlerFunc(srv.ServeHTTP))
	defer ts.Close()
	defer srv.Close()

	go srv.Serve() //nolint:errcheck

	cl := dialWS(t, ts.Listener.Addr().String(), "/")
	defer cl.conn.Close()

	<-connReady

	// Test JoinRoom syncs conn.Rooms()
	srv.JoinRoom("/", "roomA", conn)
	rooms := conn.Rooms()
	if len(rooms) != 1 || rooms[0] != "roomA" {
		t.Fatalf("expected conn.Rooms() to contain roomA, got: %v", rooms)
	}

	// Test MaxRoomsPerConn (100) limit enforcement via Server.JoinRoom
	for i := 0; i < 150; i++ {
		srv.JoinRoom("/", fmt.Sprintf("room-%d", i), conn)
	}

	if len(conn.Rooms()) > 100 {
		t.Fatalf("expected <= 100 rooms per conn, got: %d", len(conn.Rooms()))
	}

	// Test LeaveRoom
	srv.LeaveRoom("/", "roomA", conn)
	for _, r := range conn.Rooms() {
		if r == "roomA" {
			t.Fatal("roomA was not removed after LeaveRoom")
		}
	}

	// Test LeaveAllRooms
	srv.LeaveAllRooms("/", conn)
	if len(conn.Rooms()) != 0 {
		t.Fatalf("expected 0 rooms after LeaveAllRooms, got: %d", len(conn.Rooms()))
	}
}

// 6. Medium: Empty rooms garbage collection.
func TestRooms_EmptyGarbageCollection(t *testing.T) {
	srv := gsocketio.New(nil)
	var conn gsocketio.Conn
	connReady := make(chan struct{})

	srv.OnConnect("/", func(c gsocketio.Conn) error {
		conn = c
		close(connReady)
		return nil
	})

	ts := httptest.NewServer(http.HandlerFunc(srv.ServeHTTP))
	defer ts.Close()
	defer srv.Close()

	go srv.Serve() //nolint:errcheck

	cl := dialWS(t, ts.Listener.Addr().String(), "/")
	defer cl.conn.Close()

	<-connReady

	srv.JoinRoom("/", "doc-12345", conn)
	if srv.RoomLen("/", "doc-12345") != 1 {
		t.Fatalf("expected RoomLen 1, got %d", srv.RoomLen("/", "doc-12345"))
	}
	hasRoom := false
	for _, name := range srv.Rooms("/") {
		if name == "doc-12345" {
			hasRoom = true
			break
		}
	}
	if !hasRoom {
		t.Fatal("expected doc-12345 in srv.Rooms()")
	}

	// Leave room
	srv.LeaveRoom("/", "doc-12345", conn)
	if srv.RoomLen("/", "doc-12345") != 0 {
		t.Fatalf("expected RoomLen 0, got %d", srv.RoomLen("/", "doc-12345"))
	}
	for _, name := range srv.Rooms("/") {
		if name == "doc-12345" {
			t.Fatal("empty room doc-12345 was not deleted from room map")
		}
	}
}

// 7. Medium: Per-connection event concurrency limit / backpressure.
func TestEventConcurrencyLimit(t *testing.T) {
	opts := &gsocketio.Options{
		MaxEventConcurrency: 2,
	}
	srv := gsocketio.New(opts)

	var currentConcurrent int32
	var maxConcurrent int32

	srv.OnEvent("/", "work", func(c gsocketio.Conn, args []json.RawMessage) {
		curr := atomic.AddInt32(&currentConcurrent, 1)
		for {
			oldMax := atomic.LoadInt32(&maxConcurrent)
			if curr <= oldMax || atomic.CompareAndSwapInt32(&maxConcurrent, oldMax, curr) {
				break
			}
		}
		time.Sleep(30 * time.Millisecond)
		atomic.AddInt32(&currentConcurrent, -1)
	})

	ts := httptest.NewServer(http.HandlerFunc(srv.ServeHTTP))
	defer ts.Close()
	defer srv.Close()

	go srv.Serve() //nolint:errcheck

	cl := dialWS(t, ts.Listener.Addr().String(), "/")
	defer cl.conn.Close()

	// Send 6 work events
	for i := 0; i < 6; i++ {
		pkt := &packet.Packet{
			Type:      packet.TypeEvent,
			Namespace: "/",
			Data:      json.RawMessage(`["work"]`),
		}
		sioPkt, _ := packet.Encode(pkt)
		cl.sendRaw(append([]byte{'4'}, sioPkt...))
	}

	time.Sleep(250 * time.Millisecond)
	observedMax := atomic.LoadInt32(&maxConcurrent)
	if observedMax > 2 {
		t.Fatalf("max concurrent executions exceeded limit 2, got: %d", observedMax)
	}
}

// 8. Minor: Must CONNECT first check before processing events.
func TestConnectFirstOrder(t *testing.T) {
	srv := gsocketio.New(nil)
	var eventCalled bool
	srv.OnEvent("/", "bad", func(c gsocketio.Conn, args []json.RawMessage) {
		eventCalled = true
	})

	ts := httptest.NewServer(http.HandlerFunc(srv.ServeHTTP))
	defer ts.Close()
	defer srv.Close()

	go srv.Serve() //nolint:errcheck

	// Open raw WS without sending SIO CONNECT (40)
	tc, err := net.DialTimeout("tcp", ts.Listener.Addr().String(), 3*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer tc.Close()

	br := bufio.NewReader(tc)
	bw := bufio.NewWriter(tc)

	req := fmt.Sprintf(
		"GET /socket.io/?EIO=4&transport=websocket HTTP/1.1\r\n"+
			"Host: %s\r\n"+
			"Upgrade: websocket\r\n"+
			"Connection: Upgrade\r\n"+
			"Sec-WebSocket-Key: %s\r\n"+
			"Sec-WebSocket-Version: 13\r\n\r\n",
		ts.Listener.Addr().String(), wsTestKey,
	)
	fmt.Fprint(bw, req)
	bw.Flush() //nolint:errcheck

	// Read 101 response
	for {
		l, _ := br.ReadString('\n')
		if l == "\r\n" || l == "\n" || l == "" {
			break
		}
	}

	// Consume EIO open packet
	cl := &testClient{t: t, conn: tc, br: br, bw: bw}
	_, _, err = cl.readServerFrame()
	if err != nil {
		t.Fatalf("read EIO open: %v", err)
	}

	// Send an event packet "42[\"bad\"]" WITHOUT CONNECTING FIRST
	pkt := &packet.Packet{
		Type:      packet.TypeEvent,
		Namespace: "/",
		Data:      json.RawMessage(`["bad"]`),
	}
	sioPkt, _ := packet.Encode(pkt)
	cl.sendRaw(append([]byte{'4'}, sioPkt...))

	// Server must terminate the connection
	cl.conn.SetReadDeadline(time.Now().Add(1 * time.Second))
	_, _, err = cl.readServerFrame()
	if err == nil {
		// If read returned, next read should fail with EOF/closed
		_, _, err = cl.readServerFrame()
	}

	if eventCalled {
		t.Fatal("event handler was executed despite client not sending CONNECT packet first")
	}
}

func intPtr(i int) *int {
	return &i
}
