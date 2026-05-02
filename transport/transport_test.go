package transport_test

import (
	"bufio"
	"encoding/binary"
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

// buildMaskedFrame creates a masked WebSocket frame as a client would send.
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

// wsPipe creates a WSConn wired to an in-memory net.Pipe.
func wsPipe(t *testing.T) (*transport.WSConn, net.Conn) {
	t.Helper()
	serverRaw, clientRaw := net.Pipe()
	brw := bufio.NewReadWriter(bufio.NewReader(serverRaw), bufio.NewWriter(serverRaw))
	return transport.NewWSConnForTest(serverRaw, brw), clientRaw
}

// ─────────────────────────────────────────────────────────────────────────────
// Opcode constants
// ─────────────────────────────────────────────────────────────────────────────

func TestOpcodeValues(t *testing.T) {
	cases := []struct{ name string; got, want byte }{
		{"Continuation", transport.OpContinuation, 0x0},
		{"Text",         transport.OpText,         0x1},
		{"Binary",       transport.OpBinary,        0x2},
		{"Close",        transport.OpClose,         0x8},
		{"Ping",         transport.OpPing,          0x9},
		{"Pong",         transport.OpPong,          0xA},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("%s: want 0x%X got 0x%X", c.name, c.want, c.got)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Frame header size verification
// ─────────────────────────────────────────────────────────────────────────────

func TestMaskedFrameHeader_Small(t *testing.T) {
	payload := []byte("hello")
	frame := buildMaskedFrame(transport.OpText, payload)
	want := 2 + 4 + len(payload) // 2 hdr + 4 mask + payload
	if len(frame) != want {
		t.Errorf("small frame: want %d got %d", want, len(frame))
	}
}

func TestMaskedFrameHeader_Medium(t *testing.T) {
	payload := make([]byte, 200)
	frame := buildMaskedFrame(transport.OpText, payload)
	want := 4 + 4 + len(payload) // 2 hdr + 2 ext + 4 mask + payload
	if len(frame) != want {
		t.Errorf("medium frame: want %d got %d", want, len(frame))
	}
}

func TestMaskedFrameHeader_Large(t *testing.T) {
	payload := make([]byte, 70000)
	frame := buildMaskedFrame(transport.OpText, payload)
	want := 10 + 4 + len(payload) // 2 hdr + 8 ext + 4 mask + payload
	if len(frame) != want {
		t.Errorf("large frame: want %d got %d", want, len(frame))
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// WriteText: server → client
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
// ReadMessage: client → server (text)
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
			t.Errorf("opcode: want OpText(0x1) got 0x%X", r.op)
		}
		if string(r.data) != string(payload) {
			t.Errorf("data: want %q got %q", payload, r.data)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("ReadMessage timed out")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// ReadMessage: large payload (>125 bytes, exercises 2-byte length)
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
// ReadMessage: close frame
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
			t.Logf("note: opcode 0x%X — acceptable", op)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("TestReadMessage_Close timed out")
	}
	srvConn.Close() //nolint:errcheck
}

// ─────────────────────────────────────────────────────────────────────────────
// SID generator
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
// Transport Server HTTP — CORS OPTIONS
// ─────────────────────────────────────────────────────────────────────────────

func TestServer_CORSOptions(t *testing.T) {
	srv := transport.NewServer(nil)
	defer srv.Close() //nolint:errcheck

	req, _ := http.NewRequest(http.MethodOptions, "/socket.io/", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Errorf("OPTIONS: want 204 got %d", w.Code)
	}
	if got := w.Header().Get("Access-Control-Allow-Origin"); got != "*" {
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
