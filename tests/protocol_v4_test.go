package gsocketio_test

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/shishir1290/gsocketio"
	"github.com/shishir1290/gsocketio/packet"
)

func TestProtocolV4_HandshakeDetails(t *testing.T) {
	srv, addr, cleanup := newTestServer(t)
	defer cleanup()

	srv.OnConnect("/", func(c gsocketio.Conn) error {
		return nil
	})

	// Manual handshake to inspect packets
	tc, br, bw := dialRaw(t, addr)
	defer tc.Close()

	// 1. WebSocket Upgrade
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
	bw.Flush()

	// Skip response headers
	skipHeaders(t, br)

	// 2. Read EIO Open Packet
	_, eioRaw, err := readFrame(br)
	if err != nil {
		t.Fatalf("read EIO open: %v", err)
	}
	if len(eioRaw) == 0 || eioRaw[0] != '0' {
		t.Fatalf("expected EIO open, got %q", eioRaw)
	}

	// 3. Send SIO CONNECT
	connectPkt := &packet.Packet{Type: packet.TypeConnect, Namespace: "/"}
	sioPkt, _ := packet.Encode(connectPkt)
	sendFrame(bw, append([]byte{'4'}, sioPkt...))

	// 4. Read SIO CONNECT Ack and check SID
	_, ackRaw, err := readFrame(br)
	if err != nil {
		t.Fatalf("read CONNECT ack: %v", err)
	}
	if len(ackRaw) < 2 || ackRaw[0] != '4' || ackRaw[1] != '0' {
		t.Fatalf("expected 40..., got %q", ackRaw)
	}

	// The rest should be JSON
	var data map[string]string
	err = json.Unmarshal(ackRaw[2:], &data)
	if err != nil {
		t.Fatalf("failed to unmarshal CONNECT ack data: %v. Raw: %q", err, ackRaw[2:])
	}

	if data["sid"] == "" {
		t.Error("expected 'sid' in CONNECT ack data, but it's missing or empty")
	}
	t.Logf("Received sid: %s", data["sid"])
}

func TestProtocolV4_Heartbeat(t *testing.T) {
	// Set short timeout for testing
	opts := &gsocketio.Options{
		PingInterval: 500 * time.Millisecond,
		PingTimeout:  500 * time.Millisecond,
	}
	srv := gsocketio.New(opts)
	go srv.Serve()
	defer srv.Close()

	mux := http.NewServeMux()
	mux.Handle("/socket.io/", srv)
	httpSrv := httptest.NewServer(mux)
	defer httpSrv.Close()

	addr := strings.TrimPrefix(httpSrv.URL, "http://")

	tc, br, bw := dialRaw(t, addr)
	defer tc.Close()

	// Upgrade and Connect
	fmt.Fprint(bw, fmt.Sprintf("GET /socket.io/?EIO=4&transport=websocket HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n\r\n", addr, wsTestKey))
	bw.Flush()
	skipHeaders(t, br)
	readFrame(br) // open packet

	connectPkt := &packet.Packet{Type: packet.TypeConnect, Namespace: "/"}
	sioPkt, _ := packet.Encode(connectPkt)
	sendFrame(bw, append([]byte{'4'}, sioPkt...))
	readFrame(br) // connect ack

	// Now wait for heartbeat
	// In EIO4, the server should NOT send anything spontaneously.
	// We'll set a short deadline and try to read.
	tc.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	_, p, err := readFrame(br)
	tc.SetReadDeadline(time.Time{})
	if err == nil {
		if len(p) > 0 && p[0] == '2' {
			t.Error("Server sent a PING, but EIO4 should be client-initiated")
		} else {
			t.Logf("Received unexpected packet: %q", p)
		}
	} else if !strings.Contains(err.Error(), "timeout") && !strings.Contains(err.Error(), "deadline") {
		t.Logf("Expected timeout, got: %v", err)
	} else {
		t.Log("Success: server didn't send a ping within 100ms")
	}

	// Now send a client PING
	t.Log("Sending client PING...")
	sendFrame(bw, []byte{'2'})
	
	// Server should respond with PONG "3"
	t.Log("Waiting for PONG...")
	_, pong, err := readFrame(br)
	if err != nil {
		t.Fatalf("read PONG: %v", err)
	}
	t.Logf("Received PONG: %q", pong)
	if len(pong) == 0 || pong[0] != '3' {
		t.Fatalf("expected PONG '3', got %q", pong)
	}
	t.Log("Successfully received PONG from server")

	// Now stop sending pings and wait for timeout
	// Timeout is 500ms (interval) + 500ms (timeout) = 1000ms.
	// We'll wait 1200ms.
	time.Sleep(1200 * time.Millisecond)
	
	// Next read should fail or return OpClose (8)
	op, _, err := readFrame(br)
	if err == nil && op != 8 {
		t.Errorf("expected connection to be closed (OpClose=8) by server due to heartbeat timeout, but got opcode %d", op)
	} else {
		t.Logf("Connection closed as expected: op=%d err=%v", op, err)
	}
}

// Helpers duplicated/adapted from server_integration_test.go to avoid export issues or complex dependencies
func dialRaw(t *testing.T, addr string) (net.Conn, *bufio.Reader, *bufio.Writer) {
	tc, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return tc, bufio.NewReader(tc), bufio.NewWriter(tc)
}

func skipHeaders(t *testing.T, br *bufio.Reader) {
	for {
		line, err := br.ReadString('\n')
		if err != nil || line == "\r\n" || line == "\n" || line == "" {
			break
		}
	}
}

func readFrame(br *bufio.Reader) (byte, []byte, error) {
	b0, err := br.ReadByte()
	if err != nil { return 0, nil, err }
	opcode := b0 & 0x0F
	b1, err := br.ReadByte()
	if err != nil { return 0, nil, err }
	payLen := uint64(b1 & 0x7F)
	switch payLen {
	case 126:
		var ext [2]byte
		io.ReadFull(br, ext[:])
		payLen = uint64(binary.BigEndian.Uint16(ext[:]))
	case 127:
		var ext [8]byte
		io.ReadFull(br, ext[:])
		payLen = binary.BigEndian.Uint64(ext[:])
	}
	payload := make([]byte, payLen)
	_, err = io.ReadFull(br, payload)
	return opcode, payload, err
}

func sendFrame(bw *bufio.Writer, payload []byte) {
	mask := [4]byte{0x11, 0x22, 0x33, 0x44}
	masked := make([]byte, len(payload))
	for i, b := range payload {
		masked[i] = b ^ mask[i%4]
	}
	bw.WriteByte(0x80 | 0x01)
	l := len(payload)
	if l <= 125 {
		bw.WriteByte(byte(l) | 0x80)
	} else {
		// simplify for test
		bw.WriteByte(126 | 0x80)
		bw.WriteByte(byte(l >> 8))
		bw.WriteByte(byte(l))
	}
	bw.Write(mask[:])
	bw.Write(masked)
	bw.Flush()
}
