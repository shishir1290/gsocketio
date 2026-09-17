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

	tc, br, bw := dialRaw(t, addr)
	defer tc.Close()

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

	skipHeaders(t, br)

	_, eioRaw, err := readFrame(br)
	if err != nil {
		t.Fatalf("read EIO open: %v", err)
	}
	if len(eioRaw) == 0 || eioRaw[0] != '0' {
		t.Fatalf("expected EIO open, got %q", eioRaw)
	}

	connectPkt := &packet.Packet{Type: packet.TypeConnect, Namespace: "/"}
	sioPkt, _ := packet.Encode(connectPkt)
	sendFrame(bw, append([]byte{'4'}, sioPkt...))

	_, ackRaw, err := readFrame(br)
	if err != nil {
		t.Fatalf("read CONNECT ack: %v", err)
	}
	if len(ackRaw) < 2 || ackRaw[0] != '4' || ackRaw[1] != '0' {
		t.Fatalf("expected 40..., got %q", ackRaw)
	}

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

	fmt.Fprint(bw, fmt.Sprintf(
		"GET /socket.io/?EIO=4&transport=websocket HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n\r\n",
		addr, wsTestKey,
	))
	bw.Flush()
	skipHeaders(t, br)
	readFrame(br) // open packet

	connectPkt := &packet.Packet{Type: packet.TypeConnect, Namespace: "/"}
	sioPkt, _ := packet.Encode(connectPkt)
	sendFrame(bw, append([]byte{'4'}, sioPkt...))
	readFrame(br) // connect ack

	// Engine.IO v4 uses a server-driven heartbeat: the server sends PING (2),
	// and the client answers with PONG (3).
	tc.SetReadDeadline(time.Now().Add(1 * time.Second))
	_, ping, err := readFrame(br)
	tc.SetReadDeadline(time.Time{})
	if err != nil {
		t.Fatalf("read server PING: %v", err)
	}
	if len(ping) != 1 || ping[0] != '2' {
		t.Fatalf("expected Engine.IO PING '2', got %q", ping)
	}

	// Answer the heartbeat PING.
	sendFrame(bw, []byte{'3'})

	// The next heartbeat must also be answered before the timeout expires.
	tc.SetReadDeadline(time.Now().Add(1 * time.Second))
	_, ping, err = readFrame(br)
	tc.SetReadDeadline(time.Time{})
	if err != nil {
		t.Fatalf("read second server PING: %v", err)
	}
	if len(ping) != 1 || ping[0] != '2' {
		t.Fatalf("expected second Engine.IO PING '2', got %q", ping)
	}

	// Do not answer the second PING. The server should close the connection
	// after PingTimeout.
	tc.SetReadDeadline(time.Now().Add(1 * time.Second))
	op, _, err := readFrame(br)
	tc.SetReadDeadline(time.Time{})
	if err == nil && op != 8 {
		t.Fatalf("expected close frame (opcode 8), got opcode %d", op)
	}
}

func dialRaw(t *testing.T, addr string) (net.Conn, *bufio.Reader, *bufio.Writer) {
	t.Helper()
	tc, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return tc, bufio.NewReader(tc), bufio.NewWriter(tc)
}

func skipHeaders(t *testing.T, br *bufio.Reader) {
	t.Helper()
	for {
		line, err := br.ReadString('\n')
		if err != nil || line == "\r\n" || line == "\n" || line == "" {
			break
		}
	}
}

func readFrame(br *bufio.Reader) (byte, []byte, error) {
	b0, err := br.ReadByte()
	if err != nil {
		return 0, nil, err
	}
	opcode := b0 & 0x0F
	b1, err := br.ReadByte()
	if err != nil {
		return 0, nil, err
	}
	payLen := uint64(b1 & 0x7F)
	switch payLen {
	case 126:
		var ext [2]byte
		if _, err := io.ReadFull(br, ext[:]); err != nil {
			return 0, nil, err
		}
		payLen = uint64(binary.BigEndian.Uint16(ext[:]))
	case 127:
		var ext [8]byte
		if _, err := io.ReadFull(br, ext[:]); err != nil {
			return 0, nil, err
		}
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
	bw.WriteByte(0x80 | 0x01) //nolint:errcheck
	l := len(payload)
	if l <= 125 {
		bw.WriteByte(byte(l) | 0x80) //nolint:errcheck
	} else {
		bw.WriteByte(126 | 0x80)   //nolint:errcheck
		bw.WriteByte(byte(l >> 8)) //nolint:errcheck
		bw.WriteByte(byte(l))      //nolint:errcheck
	}
	bw.Write(mask[:]) //nolint:errcheck
	bw.Write(masked)  //nolint:errcheck
	bw.Flush()        //nolint:errcheck
}
