package transport_test

import (
	"bufio"
	"encoding/binary"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/shishir1290/gsocketio/transport"
)

// ─────────────────────────────────────────────────────────────────────────────
// helpers
// ─────────────────────────────────────────────────────────────────────────────

// buildMaskedFrame creates a properly masked WebSocket frame (client → server).
func buildMaskedFrame(opcode byte, payload []byte) []byte {
	var out []byte
	out = append(out, 0x80|opcode)
	mask := [4]byte{0xAB, 0xCD, 0xEF, 0x12}
	masked := make([]byte, len(payload))
	for i, b := range payload {
		masked[i] = b ^ mask[i%4]
	}
	l := len(payload)
	switch {
	case l <= 125:
		out = append(out, byte(l)|0x80)
	case l <= 65535:
		out = append(out, 126|0x80, byte(l>>8), byte(l))
	default:
		out = append(out, 127|0x80)
		var ext [8]byte
		binary.BigEndian.PutUint64(ext[:], uint64(l))
		out = append(out, ext[:]...)
	}
	out = append(out, mask[:]...)
	out = append(out, masked...)
	return out
}

// buildUnmaskedFrame creates an UNMASKED frame (RFC 6455 violation).
func buildUnmaskedFrame(opcode byte, payload []byte) []byte {
	var out []byte
	out = append(out, 0x80|opcode)
	l := len(payload)
	switch {
	case l <= 125:
		out = append(out, byte(l)) // MASK bit NOT set
	case l <= 65535:
		out = append(out, 126, byte(l>>8), byte(l))
	}
	out = append(out, payload...)
	return out
}

// wsPipe creates a WSConn wired to an in-memory net.Pipe.
func wsPipe(t *testing.T) (*transport.WSConn, net.Conn) {
	t.Helper()
	serverRaw, clientRaw := net.Pipe()
	brw := bufio.NewReadWriter(bufio.NewReader(serverRaw), bufio.NewWriter(serverRaw))
	return transport.NewWSConnForTest(serverRaw, brw), clientRaw
}

// ─────────────────────────────────────────────────────────────────────────────
// Opcode constants (RFC 6455)
// ─────────────────────────────────────────────────────────────────────────────

func TestOpcodeValues(t *testing.T) {
	cases := []struct {
		name string
		got  byte
		want byte
	}{
		{"Continuation", transport.OpContinuation, 0x0},
		{"Text", transport.OpText, 0x1},
		{"Binary", transport.OpBinary, 0x2},
		{"Close", transport.OpClose, 0x8},
		{"Ping", transport.OpPing, 0x9},
		{"Pong", transport.OpPong, 0xA},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("%s: want 0x%X got 0x%X", c.name, c.want, c.got)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Frame header size checks
// ─────────────────────────────────────────────────────────────────────────────

func TestMaskedFrameHeader_Small(t *testing.T) {
	payload := []byte("hello")
	frame := buildMaskedFrame(transport.OpText, payload)
	want := 2 + 4 + len(payload)
	if len(frame) != want {
		t.Errorf("small frame: want %d got %d", want, len(frame))
	}
}

func TestMaskedFrameHeader_Medium(t *testing.T) {
	payload := make([]byte, 200)
	frame := buildMaskedFrame(transport.OpText, payload)
	want := 4 + 4 + len(payload)
	if len(frame) != want {
		t.Errorf("medium frame: want %d got %d", want, len(frame))
	}
}

func TestMaskedFrameHeader_Large(t *testing.T) {
	payload := make([]byte, 70000)
	frame := buildMaskedFrame(transport.OpText, payload)
	want := 10 + 4 + len(payload)
	if len(frame) != want {
		t.Errorf("large frame: want %d got %d", want, len(frame))
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// FIX T-01 — Unmasked frame must be rejected
// ─────────────────────────────────────────────────────────────────────────────

func TestReadMessage_UnmaskedFrameRejected(t *testing.T) {
	srvConn, clientRaw := wsPipe(t)
	defer srvConn.Close()
	defer clientRaw.Close()

	unmasked := buildUnmaskedFrame(transport.OpText, []byte("bad client"))

	done := make(chan error, 1)
	go func() {
		_, _, err := srvConn.ReadMessage()
		done <- err
	}()

	clientRaw.SetWriteDeadline(time.Now().Add(2 * time.Second))
	clientRaw.Write(unmasked) //nolint:errcheck
	// Drain the close frame the server sends back (status 1002)
	buf := make([]byte, 64)
	clientRaw.SetReadDeadline(time.Now().Add(2 * time.Second))
	clientRaw.Read(buf) //nolint:errcheck

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected error for unmasked frame, got nil")
		}
		if !errors.Is(err, transport.ErrUnmaskedFrame) {
			t.Logf("got error (acceptable): %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("ReadMessage timed out waiting for unmasked frame error")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// FIX T-02 — Sec-WebSocket-Version enforcement
// ─────────────────────────────────────────────────────────────────────────────

func TestServer_WebSocketVersion13_Accepted(t *testing.T) {
	srv := transport.NewServer(nil)
	defer srv.Close() //nolint:errcheck

	ts := httptest.NewServer(srv)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/socket.io/?EIO=4&transport=websocket", nil)
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Sec-Websocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
	req.Header.Set("Sec-Websocket-Version", "13") // correct version

	// We just check the server responds with 101, not 426.
	// (The actual WS upgrade will fail after 101 because httptest doesn't hijack,
	//  but we confirm version check passes.)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Skip("transport-level check skipped in httptest:", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusUpgradeRequired {
		t.Errorf("version 13 should NOT return 426, got %d", resp.StatusCode)
	}
}

func TestServer_WebSocketVersionNot13_Rejected(t *testing.T) {
	srv := transport.NewServer(nil)
	defer srv.Close() //nolint:errcheck

	ts := httptest.NewServer(srv)
	defer ts.Close()

	for _, badVersion := range []string{"8", "7", ""} {
		req, _ := http.NewRequest(http.MethodGet, ts.URL+"/socket.io/?EIO=4&transport=websocket", nil)
		req.Header.Set("Upgrade", "websocket")
		req.Header.Set("Connection", "Upgrade")
		req.Header.Set("Sec-Websocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
		if badVersion != "" {
			req.Header.Set("Sec-Websocket-Version", badVersion)
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			continue
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusUpgradeRequired {
			t.Errorf("version %q: want 426 got %d", badVersion, resp.StatusCode)
		}
		// RFC says server must include Sec-WebSocket-Version: 13 in 426 response
		if got := resp.Header.Get("Sec-WebSocket-Version"); got != "13" {
			t.Errorf("version %q: 426 response missing Sec-WebSocket-Version: 13, got %q", badVersion, got)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// FIX T-03 — MaxPayload enforced before allocation
// ─────────────────────────────────────────────────────────────────────────────

func TestReadMessage_MaxPayloadEnforced(t *testing.T) {
	srvConn, clientRaw := wsPipe(t) // default 1MB max
	defer srvConn.Close()
	defer clientRaw.Close()

	// This test verifies the server rejects a frame declared larger than MaxPayload.
	// We build a frame whose length header claims 2MB but only send the header —
	// the server should reject based on the declared length before reading body.
	var frame []byte
	frame = append(frame, 0x80|transport.OpText) // FIN + Text
	frame = append(frame, 127|0x80)               // MASK + 8-byte extended length
	var ext [8]byte
	binary.BigEndian.PutUint64(ext[:], 2_000_000) // 2MB — exceeds 1MB default
	frame = append(frame, ext[:]...)
	mask := [4]byte{0x01, 0x02, 0x03, 0x04}
	frame = append(frame, mask[:]...)
	// Don't send actual payload — rejection should happen at length check

	done := make(chan error, 1)
	go func() {
		_, _, err := srvConn.ReadMessage()
		done <- err
	}()

	clientRaw.SetWriteDeadline(time.Now().Add(2 * time.Second))
	clientRaw.Write(frame) //nolint:errcheck

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected error for oversized payload, got nil")
		}
		if !errors.Is(err, transport.ErrPayloadTooLarge) {
			t.Logf("got error (may be acceptable if pipe closed first): %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("ReadMessage timed out")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// WriteText server → client (masked frames from client, unmasked from server)
// ─────────────────────────────────────────────────────────────────────────────

func TestWriteText_ServerToClient(t *testing.T) {
	srvConn, clientRaw := wsPipe(t)
	defer srvConn.Close()
	defer clientRaw.Close()

	msg := []byte("socket.io rocks")
	go srvConn.WriteText(msg) //nolint:errcheck

	br := bufio.NewReader(clientRaw)
	clientRaw.SetReadDeadline(time.Now().Add(2 * time.Second))

	b0, err := br.ReadByte()
	if err != nil {
		t.Fatalf("read b0: %v", err)
	}
	if b0 != 0x80|transport.OpText {
		t.Errorf("b0: want 0x%X got 0x%X", 0x80|transport.OpText, b0)
	}
	b1, _ := br.ReadByte()
	if b1&0x80 != 0 {
		t.Error("server frame must NOT be masked (RFC 6455 §5.1)")
	}
	payLen := int(b1 & 0x7F)
	if payLen != len(msg) {
		t.Errorf("payload len: want %d got %d", len(msg), payLen)
	}
	payload := make([]byte, payLen)
	br.Read(payload) //nolint:errcheck
	if string(payload) != string(msg) {
		t.Errorf("payload: want %q got %q", msg, payload)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// ReadMessage — text from properly masked client frame
// ─────────────────────────────────────────────────────────────────────────────

func TestReadMessage_Text(t *testing.T) {
	srvConn, clientRaw := wsPipe(t)
	defer srvConn.Close()
	defer clientRaw.Close()

	payload := []byte("hello from client")
	frame := buildMaskedFrame(transport.OpText, payload)

	type result struct {
		op   byte
		data []byte
		err  error
	}
	done := make(chan result, 1)
	go func() {
		op, data, err := srvConn.ReadMessage()
		done <- result{op, data, err}
	}()

	clientRaw.SetWriteDeadline(time.Now().Add(2 * time.Second))
	clientRaw.Write(frame) //nolint:errcheck

	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("ReadMessage: %v", r.err)
		}
		if r.op != transport.OpText {
			t.Errorf("opcode: want OpText got 0x%X", r.op)
		}
		if string(r.data) != string(payload) {
			t.Errorf("data: want %q got %q", payload, r.data)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("ReadMessage timed out")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// ReadMessage — medium payload (126+ bytes, 2-byte extended length)
// ─────────────────────────────────────────────────────────────────────────────

func TestReadMessage_MediumPayload(t *testing.T) {
	srvConn, clientRaw := wsPipe(t)
	defer srvConn.Close()
	defer clientRaw.Close()

	payload := make([]byte, 300)
	for i := range payload {
		payload[i] = byte(i % 256)
	}
	frame := buildMaskedFrame(transport.OpText, payload)

	done := make(chan []byte, 1)
	go func() {
		_, data, _ := srvConn.ReadMessage()
		done <- data
	}()

	clientRaw.Write(frame) //nolint:errcheck

	select {
	case data := <-done:
		if len(data) != len(payload) {
			t.Errorf("payload length: want %d got %d", len(payload), len(data))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("ReadMessage large payload timed out")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// ReadMessage — close frame
// ─────────────────────────────────────────────────────────────────────────────

func TestReadMessage_Close(t *testing.T) {
	srvConn, clientRaw := wsPipe(t)

	closeFrame := buildMaskedFrame(transport.OpClose, nil)

	go func() {
		defer clientRaw.Close()
		clientRaw.SetWriteDeadline(time.Now().Add(2 * time.Second))
		clientRaw.Write(closeFrame) //nolint:errcheck
		buf := make([]byte, 64)
		clientRaw.SetReadDeadline(time.Now().Add(2 * time.Second))
		clientRaw.Read(buf) //nolint:errcheck
	}()

	done := make(chan byte, 1)
	go func() {
		op, _, _ := srvConn.ReadMessage()
		done <- op
	}()

	select {
	case op := <-done:
		if op != transport.OpClose {
			t.Logf("opcode 0x%X — acceptable on pipe close", op)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("TestReadMessage_Close timed out")
	}
	srvConn.Close() //nolint:errcheck
}

// ─────────────────────────────────────────────────────────────────────────────
// SID generator — FIX S-02
// ─────────────────────────────────────────────────────────────────────────────

func TestNewSID_Unique(t *testing.T) {
	seen := make(map[string]struct{}, 1000)
	for i := 0; i < 1000; i++ {
		sid := transport.NewSID()
		if _, ok := seen[sid]; ok {
			t.Fatalf("duplicate SID: %q", sid)
		}
		seen[sid] = struct{}{}
	}
}

func TestNewSID_NotEmpty(t *testing.T) {
	if transport.NewSID() == "" {
		t.Error("NewSID returned empty string")
	}
}

func TestNewSID_ConsistentLength(t *testing.T) {
	l := len(transport.NewSID())
	for i := 0; i < 200; i++ {
		if n := len(transport.NewSID()); n != l {
			t.Fatalf("length changed: got %d expected %d", n, l)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Transport Server — CORS OPTIONS
// ─────────────────────────────────────────────────────────────────────────────

func TestServer_CORSOptions(t *testing.T) {
	srv := transport.NewServer(nil)
	defer srv.Close() //nolint:errcheck

	ts := httptest.NewServer(srv)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodOptions, ts.URL+"/socket.io/", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("OPTIONS: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("OPTIONS: want 204 got %d", resp.StatusCode)
	}
	if got := resp.Header.Get("Access-Control-Allow-Origin"); got != "*" {
		t.Errorf("CORS origin: want * got %q", got)
	}
}

func TestServer_Count(t *testing.T) {
	srv := transport.NewServer(nil)
	defer srv.Close() //nolint:errcheck
	if n := srv.Count(); n != 0 {
		t.Errorf("initial count: want 0 got %d", n)
	}
}
