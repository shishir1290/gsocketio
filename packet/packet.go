// Package packet implements the Socket.IO v4 packet protocol from scratch.
//
// Wire format:
//   <type>[<namespace>,][<ack_id>][<json_data>]
//
// Packet types:
//   0  CONNECT      — client joins a namespace
//   1  DISCONNECT   — client leaves a namespace
//   2  EVENT        — named event with optional args
//   3  ACK          — acknowledgement of an event
//   4  CONNECT_ERROR — server rejects connection
//
// Examples:
//   "0"                     → CONNECT to "/"
//   "0/admin,"              → CONNECT to "/admin"
//   "2["chat","hello"]"     → EVENT "chat" with arg "hello" on "/"
//   "2/admin,["action",1]"  → EVENT on "/admin"
//   "37["ack data"]"        → ACK id=7 with data
package packet

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// ─────────────────────────────────────────────────────────────────────────────
// Type
// ─────────────────────────────────────────────────────────────────────────────

// Type identifies a Socket.IO packet type.
type Type byte

const (
	TypeConnect      Type = '0'
	TypeDisconnect   Type = '1'
	TypeEvent        Type = '2'
	TypeAck          Type = '3'
	TypeConnectError Type = '4'
)

func (t Type) String() string {
	switch t {
	case TypeConnect:
		return "CONNECT"
	case TypeDisconnect:
		return "DISCONNECT"
	case TypeEvent:
		return "EVENT"
	case TypeAck:
		return "ACK"
	case TypeConnectError:
		return "CONNECT_ERROR"
	default:
		return fmt.Sprintf("UNKNOWN(%c)", t)
	}
}

// Valid reports whether t is a known packet type.
func (t Type) Valid() bool {
	switch t {
	case TypeConnect, TypeDisconnect, TypeEvent, TypeAck, TypeConnectError:
		return true
	}
	return false
}

// ─────────────────────────────────────────────────────────────────────────────
// Packet
// ─────────────────────────────────────────────────────────────────────────────

// Packet is a decoded Socket.IO packet.
type Packet struct {
	Type      Type
	Namespace string          // always starts with "/"
	ID        *int            // ack id; nil means no acknowledgement requested
	Data      json.RawMessage // raw JSON payload (array for EVENT/ACK, object for CONNECT)
}

// ─────────────────────────────────────────────────────────────────────────────
// Errors
// ─────────────────────────────────────────────────────────────────────────────

// ErrInvalidPacket is returned when a wire message cannot be decoded.
type ErrInvalidPacket struct {
	Reason string
	Raw    string
}

func (e *ErrInvalidPacket) Error() string {
	return fmt.Sprintf("packet: invalid packet %q: %s", e.Raw, e.Reason)
}

func invalid(raw, reason string) error {
	return &ErrInvalidPacket{Reason: reason, Raw: raw}
}

// ─────────────────────────────────────────────────────────────────────────────
// Encode
// ─────────────────────────────────────────────────────────────────────────────

// Encode serialises a Packet to its wire representation.
//
// Rules:
//  1. First byte is the type digit.
//  2. If namespace is not "/", write namespace then ",".
//  3. If ID is set, write the decimal ack id.
//  4. Write JSON data if present.
func Encode(p *Packet) ([]byte, error) {
	if !p.Type.Valid() {
		return nil, fmt.Errorf("packet: unknown type %v", p.Type)
	}
	ns := p.Namespace
	if ns == "" {
		ns = "/"
	}

	var buf bytes.Buffer
	buf.WriteByte(byte(p.Type))

	if ns != "/" {
		buf.WriteString(ns)
		buf.WriteByte(',')
	}

	if p.ID != nil {
		buf.WriteString(strconv.Itoa(*p.ID))
	}

	if len(p.Data) > 0 {
		buf.Write(p.Data)
	}

	return buf.Bytes(), nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Decode
// ─────────────────────────────────────────────────────────────────────────────

// Decode parses a raw wire message into a Packet.
func Decode(raw []byte) (*Packet, error) {
	s := string(raw)
	if len(s) == 0 {
		return nil, invalid(s, "empty message")
	}

	p := &Packet{Namespace: "/"}

	// ── 1. Type byte ─────────────────────────────────────────────────────────
	p.Type = Type(s[0])
	if !p.Type.Valid() {
		return nil, invalid(s, "unknown type")
	}
	rest := s[1:]

	// ── 2. Namespace (optional, only present when not "/") ───────────────────
	if strings.HasPrefix(rest, "/") {
		idx := strings.IndexByte(rest, ',')
		if idx == -1 {
			// namespace only, no data
			p.Namespace = rest
			return p, nil
		}
		p.Namespace = rest[:idx]
		rest = rest[idx+1:]
	}

	// ── 3. Ack ID (sequence of digits before '[' or '{') ─────────────────────
	i := 0
	for i < len(rest) && rest[i] >= '0' && rest[i] <= '9' {
		i++
	}
	if i > 0 {
		id, err := strconv.Atoi(rest[:i])
		if err != nil {
			return nil, invalid(s, "bad ack id: "+err.Error())
		}
		p.ID = &id
		rest = rest[i:]
	}

	// ── 4. JSON data ──────────────────────────────────────────────────────────
	if len(rest) > 0 {
		p.Data = json.RawMessage(rest)
		// Validate JSON so callers can trust it.
		if !json.Valid(p.Data) {
			return nil, invalid(s, "invalid JSON data")
		}
	}

	return p, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Event helpers
// ─────────────────────────────────────────────────────────────────────────────

// EventName extracts the event name from an EVENT packet's JSON array.
// EVENT data format: ["eventName", arg1, arg2, ...]
func EventName(data json.RawMessage) (string, error) {
	if len(data) == 0 {
		return "", errors.New("packet: empty data")
	}
	var arr []json.RawMessage
	if err := json.Unmarshal(data, &arr); err != nil {
		return "", fmt.Errorf("packet: data is not a JSON array: %w", err)
	}
	if len(arr) == 0 {
		return "", errors.New("packet: empty event array")
	}
	var name string
	if err := json.Unmarshal(arr[0], &name); err != nil {
		return "", fmt.Errorf("packet: event name is not a string: %w", err)
	}
	return name, nil
}

// EventArgs returns the argument elements (everything after the event name).
func EventArgs(data json.RawMessage) ([]json.RawMessage, error) {
	var arr []json.RawMessage
	if err := json.Unmarshal(data, &arr); err != nil {
		return nil, fmt.Errorf("packet: data is not a JSON array: %w", err)
	}
	if len(arr) <= 1 {
		return nil, nil
	}
	return arr[1:], nil
}

// BuildEventData encodes an event name and zero or more arguments into the
// JSON array expected as packet Data.
func BuildEventData(event string, args ...interface{}) (json.RawMessage, error) {
	payload := make([]interface{}, 0, len(args)+1)
	payload = append(payload, event)
	payload = append(payload, args...)
	b, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("packet: marshal event data: %w", err)
	}
	return json.RawMessage(b), nil
}

// BuildAckData encodes ack return values into a JSON array.
func BuildAckData(values ...interface{}) (json.RawMessage, error) {
	if len(values) == 0 {
		return json.RawMessage("[]"), nil
	}
	b, err := json.Marshal(values)
	if err != nil {
		return nil, fmt.Errorf("packet: marshal ack data: %w", err)
	}
	return json.RawMessage(b), nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Namespace helpers
// ─────────────────────────────────────────────────────────────────────────────

// NormalizeNS returns a canonical namespace string that always starts with "/".
func NormalizeNS(ns string) string {
	ns = strings.TrimSpace(ns)
	if ns == "" {
		return "/"
	}
	if !strings.HasPrefix(ns, "/") {
		return "/" + ns
	}
	return ns
}
