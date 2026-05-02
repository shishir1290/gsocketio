package packet_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/shishir1290/gsocketio/packet"
)

// ─────────────────────────────────────────────────────────────────────────────
// helpers
// ─────────────────────────────────────────────────────────────────────────────

func mustEncode(t *testing.T, p *packet.Packet) []byte {
	t.Helper()
	b, err := packet.Encode(p)
	if err != nil {
		t.Fatalf("Encode(%+v): %v", p, err)
	}
	return b
}

func mustDecode(t *testing.T, raw []byte) *packet.Packet {
	t.Helper()
	p, err := packet.Decode(raw)
	if err != nil {
		t.Fatalf("Decode(%q): %v", raw, err)
	}
	return p
}

func intPtr(n int) *int { return &n }

// ─────────────────────────────────────────────────────────────────────────────
// Type.String and Type.Valid
// ─────────────────────────────────────────────────────────────────────────────

func TestTypeString(t *testing.T) {
	cases := []struct {
		tp   packet.Type
		want string
	}{
		{packet.TypeConnect, "CONNECT"},
		{packet.TypeDisconnect, "DISCONNECT"},
		{packet.TypeEvent, "EVENT"},
		{packet.TypeAck, "ACK"},
		{packet.TypeConnectError, "CONNECT_ERROR"},
		{packet.Type('Z'), "UNKNOWN(Z)"},
	}
	for _, c := range cases {
		if got := c.tp.String(); got != c.want {
			t.Errorf("Type(%c).String() = %q; want %q", c.tp, got, c.want)
		}
	}
}

func TestTypeValid(t *testing.T) {
	valid := []packet.Type{
		packet.TypeConnect, packet.TypeDisconnect,
		packet.TypeEvent, packet.TypeAck, packet.TypeConnectError,
	}
	for _, tp := range valid {
		if !tp.Valid() {
			t.Errorf("Type(%c).Valid() should be true", tp)
		}
	}
	if packet.Type('9').Valid() {
		t.Error("Type('9').Valid() should be false")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Encode round-trips
// ─────────────────────────────────────────────────────────────────────────────

func TestEncodeDecodeConnect_Root(t *testing.T) {
	p := &packet.Packet{Type: packet.TypeConnect, Namespace: "/"}
	raw := mustEncode(t, p)
	got := mustDecode(t, raw)

	if got.Type != packet.TypeConnect {
		t.Errorf("type: want CONNECT got %v", got.Type)
	}
	if got.Namespace != "/" {
		t.Errorf("namespace: want / got %q", got.Namespace)
	}
	if got.ID != nil {
		t.Errorf("id: want nil got %v", got.ID)
	}
}

func TestEncodeDecodeConnect_CustomNS(t *testing.T) {
	p := &packet.Packet{Type: packet.TypeConnect, Namespace: "/admin"}
	raw := mustEncode(t, p)

	// Wire must contain namespace + comma
	if !strings.Contains(string(raw), "/admin,") {
		t.Errorf("expected /admin, in wire: %q", raw)
	}

	got := mustDecode(t, raw)
	if got.Namespace != "/admin" {
		t.Errorf("namespace: want /admin got %q", got.Namespace)
	}
}

func TestEncodeDecodeDisconnect(t *testing.T) {
	p := &packet.Packet{Type: packet.TypeDisconnect, Namespace: "/chat"}
	got := mustDecode(t, mustEncode(t, p))
	if got.Type != packet.TypeDisconnect {
		t.Errorf("want DISCONNECT got %v", got.Type)
	}
	if got.Namespace != "/chat" {
		t.Errorf("namespace: want /chat got %q", got.Namespace)
	}
}

func TestEncodeDecodeEvent_NoArgs(t *testing.T) {
	data, err := packet.BuildEventData("ping")
	if err != nil {
		t.Fatalf("BuildEventData: %v", err)
	}
	p := &packet.Packet{Type: packet.TypeEvent, Namespace: "/", Data: data}
	got := mustDecode(t, mustEncode(t, p))

	if got.Type != packet.TypeEvent {
		t.Errorf("type: want EVENT got %v", got.Type)
	}
	name, err := packet.EventName(got.Data)
	if err != nil {
		t.Fatalf("EventName: %v", err)
	}
	if name != "ping" {
		t.Errorf("event: want ping got %q", name)
	}
	args, _ := packet.EventArgs(got.Data)
	if len(args) != 0 {
		t.Errorf("args: want 0 got %d", len(args))
	}
}

func TestEncodeDecodeEvent_StringArg(t *testing.T) {
	data, _ := packet.BuildEventData("chat", "hello world")
	p := &packet.Packet{Type: packet.TypeEvent, Namespace: "/", Data: data}
	got := mustDecode(t, mustEncode(t, p))

	args, _ := packet.EventArgs(got.Data)
	if len(args) != 1 {
		t.Fatalf("args: want 1 got %d", len(args))
	}
	var s string
	json.Unmarshal(args[0], &s)
	if s != "hello world" {
		t.Errorf("arg0: want 'hello world' got %q", s)
	}
}

func TestEncodeDecodeEvent_MultipleArgs(t *testing.T) {
	type Payload struct {
		User string `json:"user"`
		Text string `json:"text"`
	}
	data, _ := packet.BuildEventData("message", Payload{User: "alice", Text: "hi"}, 42, true)
	p := &packet.Packet{Type: packet.TypeEvent, Namespace: "/", Data: data}
	got := mustDecode(t, mustEncode(t, p))

	args, _ := packet.EventArgs(got.Data)
	if len(args) != 3 {
		t.Fatalf("args: want 3 got %d", len(args))
	}
	var pl Payload
	json.Unmarshal(args[0], &pl)
	if pl.User != "alice" || pl.Text != "hi" {
		t.Errorf("arg0: %+v", pl)
	}
	var n int
	json.Unmarshal(args[1], &n)
	if n != 42 {
		t.Errorf("arg1: want 42 got %d", n)
	}
	var b bool
	json.Unmarshal(args[2], &b)
	if !b {
		t.Error("arg2: want true")
	}
}

func TestEncodeDecodeEvent_WithAckID(t *testing.T) {
	data, _ := packet.BuildEventData("update", 99)
	p := &packet.Packet{
		Type:      packet.TypeEvent,
		Namespace: "/admin",
		ID:        intPtr(7),
		Data:      data,
	}
	raw := mustEncode(t, p)
	got := mustDecode(t, raw)

	if got.Namespace != "/admin" {
		t.Errorf("namespace: want /admin got %q", got.Namespace)
	}
	if got.ID == nil || *got.ID != 7 {
		t.Errorf("ack id: want 7 got %v", got.ID)
	}
}

func TestEncodeDecodeEvent_AckIDZero(t *testing.T) {
	data, _ := packet.BuildEventData("ping")
	p := &packet.Packet{Type: packet.TypeEvent, Namespace: "/", ID: intPtr(0), Data: data}
	got := mustDecode(t, mustEncode(t, p))
	if got.ID == nil || *got.ID != 0 {
		t.Errorf("ack id: want 0 got %v", got.ID)
	}
}

func TestEncodeDecodeAck(t *testing.T) {
	ackData, _ := packet.BuildAckData("done", 1)
	p := &packet.Packet{
		Type:      packet.TypeAck,
		Namespace: "/",
		ID:        intPtr(5),
		Data:      ackData,
	}
	got := mustDecode(t, mustEncode(t, p))
	if got.Type != packet.TypeAck {
		t.Errorf("type: want ACK got %v", got.Type)
	}
	if got.ID == nil || *got.ID != 5 {
		t.Errorf("ack id: want 5 got %v", got.ID)
	}
}

func TestEncodeDecodeConnectError(t *testing.T) {
	data, _ := json.Marshal(map[string]string{"message": "not allowed"})
	p := &packet.Packet{
		Type:      packet.TypeConnectError,
		Namespace: "/private",
		Data:      data,
	}
	got := mustDecode(t, mustEncode(t, p))
	if got.Type != packet.TypeConnectError {
		t.Errorf("type: want CONNECT_ERROR got %v", got.Type)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Encode error cases
// ─────────────────────────────────────────────────────────────────────────────

func TestEncodeUnknownType(t *testing.T) {
	_, err := packet.Encode(&packet.Packet{Type: packet.Type('Z'), Namespace: "/"})
	if err == nil {
		t.Error("expected error for unknown type")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Decode error cases
// ─────────────────────────────────────────────────────────────────────────────

func TestDecodeEmpty(t *testing.T) {
	_, err := packet.Decode([]byte{})
	if err == nil {
		t.Error("expected error for empty input")
	}
}

func TestDecodeUnknownType(t *testing.T) {
	_, err := packet.Decode([]byte("9hello"))
	if err == nil {
		t.Error("expected error for unknown type '9'")
	}
}

func TestDecodeInvalidJSON(t *testing.T) {
	_, err := packet.Decode([]byte("2notjson"))
	if err == nil {
		t.Error("expected error for invalid JSON data")
	}
}

func TestDecodeBadAckID(t *testing.T) {
	// Manually craft a packet where digit run is too long.
	// "2999999999999999999999[...]" — overflow int
	// Actually strconv.Atoi returns error on overflow, which Decode propagates.
	raw := []byte("2" + strings.Repeat("9", 30) + `["ev"]`)
	_, err := packet.Decode(raw)
	if err == nil {
		t.Error("expected error for overflowed ack id")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// EventName errors
// ─────────────────────────────────────────────────────────────────────────────

func TestEventNameEmpty(t *testing.T) {
	_, err := packet.EventName(nil)
	if err == nil {
		t.Error("expected error for nil data")
	}
}

func TestEventNameNotArray(t *testing.T) {
	_, err := packet.EventName(json.RawMessage(`"string"`))
	if err == nil {
		t.Error("expected error for non-array")
	}
}

func TestEventNameEmptyArray(t *testing.T) {
	_, err := packet.EventName(json.RawMessage(`[]`))
	if err == nil {
		t.Error("expected error for empty array")
	}
}

func TestEventNameFirstNotString(t *testing.T) {
	_, err := packet.EventName(json.RawMessage(`[42]`))
	if err == nil {
		t.Error("expected error when first element is not a string")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// BuildAckData
// ─────────────────────────────────────────────────────────────────────────────

func TestBuildAckDataEmpty(t *testing.T) {
	data, err := packet.BuildAckData()
	if err != nil {
		t.Fatalf("BuildAckData: %v", err)
	}
	if string(data) != "[]" {
		t.Errorf("empty ack: want [] got %s", data)
	}
}

func TestBuildAckDataValues(t *testing.T) {
	data, err := packet.BuildAckData("ok", 200)
	if err != nil {
		t.Fatalf("BuildAckData: %v", err)
	}
	var arr []json.RawMessage
	json.Unmarshal(data, &arr)
	if len(arr) != 2 {
		t.Errorf("want 2 elements got %d", len(arr))
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// NormalizeNS
// ─────────────────────────────────────────────────────────────────────────────

func TestNormalizeNS(t *testing.T) {
	cases := []struct{ in, want string }{
		{"", "/"},
		{"/", "/"},
		{"chat", "/chat"},
		{"/chat", "/chat"},
		{"  /admin  ", "/admin"},
		{"   ", "/"},
	}
	for _, c := range cases {
		got := packet.NormalizeNS(c.in)
		if got != c.want {
			t.Errorf("NormalizeNS(%q) = %q; want %q", c.in, got, c.want)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Large payload round-trip
// ─────────────────────────────────────────────────────────────────────────────

func TestLargePayload(t *testing.T) {
	big := strings.Repeat("A", 100_000)
	data, _ := packet.BuildEventData("blob", big)
	p := &packet.Packet{Type: packet.TypeEvent, Namespace: "/", Data: data}
	got := mustDecode(t, mustEncode(t, p))
	args, _ := packet.EventArgs(got.Data)
	var s string
	json.Unmarshal(args[0], &s)
	if len(s) != 100_000 {
		t.Errorf("payload length: want 100000 got %d", len(s))
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// EventArgs edge cases
// ─────────────────────────────────────────────────────────────────────────────

func TestEventArgsNoArgs(t *testing.T) {
	data, _ := packet.BuildEventData("ping")
	args, err := packet.EventArgs(data)
	if err != nil {
		t.Fatalf("EventArgs: %v", err)
	}
	if len(args) != 0 {
		t.Errorf("want 0 args got %d", len(args))
	}
}

func TestEventArgsInvalidJSON(t *testing.T) {
	_, err := packet.EventArgs(json.RawMessage(`not-json`))
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Namespace-only packet (no data)
// ─────────────────────────────────────────────────────────────────────────────

func TestDecodeNamespaceOnly(t *testing.T) {
	// "0/chat" — CONNECT to /chat with no data
	got, err := packet.Decode([]byte("0/chat"))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.Namespace != "/chat" {
		t.Errorf("namespace: want /chat got %q", got.Namespace)
	}
	if got.Type != packet.TypeConnect {
		t.Errorf("type: want CONNECT got %v", got.Type)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Default namespace when empty
// ─────────────────────────────────────────────────────────────────────────────

func TestEncodeEmptyNamespaceDefaultsToRoot(t *testing.T) {
	p := &packet.Packet{Type: packet.TypeConnect, Namespace: ""}
	raw := mustEncode(t, p)
	// Root namespace is not included in wire
	if strings.Contains(string(raw), "/") {
		t.Errorf("root namespace should not appear in wire: %q", raw)
	}
	got := mustDecode(t, raw)
	if got.Namespace != "/" {
		t.Errorf("decoded namespace: want / got %q", got.Namespace)
	}
}
