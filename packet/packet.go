// Package packet implements the Socket.IO v4 packet protocol from scratch.
package packet

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

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
	case TypeConnect: return "CONNECT"
	case TypeDisconnect: return "DISCONNECT"
	case TypeEvent: return "EVENT"
	case TypeAck: return "ACK"
	case TypeConnectError: return "CONNECT_ERROR"
	default: return fmt.Sprintf("UNKNOWN(%c)", t)
	}
}

func (t Type) Valid() bool {
	switch t {
	case TypeConnect, TypeDisconnect, TypeEvent, TypeAck, TypeConnectError:
		return true
	default:
		return false
	}
}

type Packet struct {
	Type Type
	Namespace string
	ID *uint64
	Data json.RawMessage
}

type ErrInvalidPacket struct { Reason string; Raw string }
func (e *ErrInvalidPacket) Error() string { return fmt.Sprintf("packet: invalid packet %q: %s", e.Raw, e.Reason) }
func invalid(raw, reason string) error { return &ErrInvalidPacket{Reason: reason, Raw: raw} }

func Encode(p *Packet) ([]byte, error) {
	if p == nil || !p.Type.Valid() { return nil, fmt.Errorf("packet: unknown type") }
	ns := p.Namespace
	if ns == "" { ns = "/" }
	var buf bytes.Buffer
	buf.WriteByte(byte(p.Type))
	if ns != "/" { buf.WriteString(ns); buf.WriteByte(',') }
	if p.ID != nil { buf.WriteString(strconv.FormatUint(*p.ID, 10)) }
	if len(p.Data) > 0 { buf.Write(p.Data) }
	return buf.Bytes(), nil
}

func Decode(raw []byte) (*Packet, error) {
	s := string(raw)
	if s == "" { return nil, invalid(s, "empty message") }
	p := &Packet{Namespace: "/", Type: Type(s[0])}
	if !p.Type.Valid() { return nil, invalid(s, "unknown type") }
	rest := s[1:]
	if strings.HasPrefix(rest, "/") {
		idx := strings.IndexByte(rest, ',')
		if idx < 0 { return nil, invalid(s, "namespace is missing comma") }
		p.Namespace = rest[:idx]
		rest = rest[idx+1:]
		if p.Namespace == "/" { return nil, invalid(s, "default namespace must not be encoded explicitly") }
	}
	i := 0
	for i < len(rest) && rest[i] >= '0' && rest[i] <= '9' { i++ }
	if i > 0 {
		id, err := strconv.ParseUint(rest[:i], 10, 64)
		if err != nil { return nil, invalid(s, "bad ack id: "+err.Error()) }
		p.ID = &id
		rest = rest[i:]
	}
	if len(rest) > 0 {
		p.Data = json.RawMessage(rest)
		if !json.Valid(p.Data) { return nil, invalid(s, "invalid JSON data") }
	}
	return p, nil
}

func EventName(data json.RawMessage) (string, error) {
	if len(data) == 0 { return "", errors.New("packet: empty data") }
	var arr []json.RawMessage
	if err := json.Unmarshal(data, &arr); err != nil { return "", fmt.Errorf("packet: data is not a JSON array: %w", err) }
	if len(arr) == 0 { return "", errors.New("packet: empty event array") }
	var name string
	if err := json.Unmarshal(arr[0], &name); err != nil { return "", fmt.Errorf("packet: event name is not a string: %w", err) }
	if name == "" { return "", errors.New("packet: empty event name") }
	return name, nil
}

func EventArgs(data json.RawMessage) ([]json.RawMessage, error) {
	var arr []json.RawMessage
	if err := json.Unmarshal(data, &arr); err != nil { return nil, fmt.Errorf("packet: data is not a JSON array: %w", err) }
	if len(arr) <= 1 { return nil, nil }
	return arr[1:], nil
}

func BuildEventData(event string, args ...interface{}) (json.RawMessage, error) {
	payload := make([]interface{}, 0, len(args)+1)
	payload = append(payload, event)
	payload = append(payload, args...)
	b, err := json.Marshal(payload)
	if err != nil { return nil, fmt.Errorf("packet: marshal event data: %w", err) }
	return json.RawMessage(b), nil
}

func BuildAckData(values ...interface{}) (json.RawMessage, error) {
	if len(values) == 0 { return json.RawMessage("[]"), nil }
	b, err := json.Marshal(values)
	if err != nil { return nil, fmt.Errorf("packet: marshal ack data: %w", err) }
	return json.RawMessage(b), nil
}

func NormalizeNS(ns string) string {
	ns = strings.TrimSpace(ns)
	if ns == "" { return "/" }
	if !strings.HasPrefix(ns, "/") { return "/" + ns }
	return ns
}
