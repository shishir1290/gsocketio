package packet

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
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
	TypeBinaryEvent  Type = '5'
	TypeBinaryAck    Type = '6'
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
	case TypeBinaryEvent:
		return "BINARY_EVENT"
	case TypeBinaryAck:
		return "BINARY_ACK"
	default:
		return fmt.Sprintf("UNKNOWN(%c)", t)
	}
}

func (t Type) Valid() bool    { return t >= TypeConnect && t <= TypeBinaryAck }
func (t Type) IsBinary() bool { return t == TypeBinaryEvent || t == TypeBinaryAck }

type Packet struct {
	Type        Type
	Namespace   string
	ID          *int
	Data        json.RawMessage
	Attachments int
}

type ErrInvalidPacket struct {
	Reason string
	Raw    string
}

func (e *ErrInvalidPacket) Error() string {
	return fmt.Sprintf("packet: invalid packet %q: %s", e.Raw, e.Reason)
}
func invalid(raw, reason string) error { return &ErrInvalidPacket{Reason: reason, Raw: raw} }

func Encode(p *Packet) ([]byte, error) {
	if p == nil || !p.Type.Valid() {
		return nil, errors.New("packet: unknown type")
	}
	if p.Attachments < 0 || p.Attachments > 1024 {
		return nil, errors.New("packet: invalid attachment count")
	}
	if p.Type.IsBinary() && p.Attachments == 0 {
		return nil, errors.New("packet: binary packet requires attachments")
	}
	if !p.Type.IsBinary() && p.Attachments != 0 {
		return nil, errors.New("packet: non-binary packet cannot have attachments")
	}
	if p.ID != nil && *p.ID < 0 {
		return nil, errors.New("packet: negative ack id")
	}
	if (p.Type == TypeConnect || p.Type == TypeDisconnect || p.Type == TypeConnectError) && p.ID != nil {
		return nil, errors.New("packet: ack id not allowed for this packet type")
	}

	ns := NormalizeNS(p.Namespace)
	var b bytes.Buffer
	b.WriteByte(byte(p.Type))
	if p.Type.IsBinary() {
		b.WriteString(strconv.Itoa(p.Attachments))
		b.WriteByte('-')
	}
	if ns != "/" {
		if !strings.HasPrefix(ns, "/") || strings.ContainsAny(ns, "\x00\r\n") {
			return nil, errors.New("packet: invalid namespace")
		}
		b.WriteString(ns)
		b.WriteByte(',')
	}
	if p.ID != nil {
		b.WriteString(strconv.Itoa(*p.ID))
	}
	if len(p.Data) > 0 {
		b.Write(p.Data)
	}
	return b.Bytes(), nil
}

func Decode(raw []byte) (*Packet, error) {
	if len(raw) == 0 {
		return nil, invalid("", "empty message")
	}
	if len(raw) > 1<<20 {
		return nil, invalid(string(raw[:64]), "packet too large")
	}
	p := &Packet{Namespace: "/", Type: Type(raw[0])}
	if !p.Type.Valid() {
		return nil, invalid(string(raw), "unknown type")
	}
	rest := string(raw[1:])
	if p.Type.IsBinary() {
		dash := strings.IndexByte(rest, '-')
		if dash <= 0 {
			return nil, invalid(string(raw), "missing attachment count")
		}
		n, err := strconv.Atoi(rest[:dash])
		if err != nil || n <= 0 || n > 1024 {
			return nil, invalid(string(raw), "invalid attachment count")
		}
		p.Attachments = n
		rest = rest[dash+1:]
	}
	if strings.HasPrefix(rest, "/") {
		idx := strings.IndexByte(rest, ',')
		if idx >= 0 {
			p.Namespace = rest[:idx]
			rest = rest[idx+1:]
		} else {
			p.Namespace = rest
			rest = ""
		}
		if p.Namespace == "" || strings.ContainsAny(p.Namespace, "\x00\r\n") {
			return nil, invalid(string(raw), "invalid namespace")
		}
	}
	if p.Type.IsBinary() || p.Type == TypeEvent || p.Type == TypeAck {
		i := 0
		for i < len(rest) && rest[i] >= '0' && rest[i] <= '9' {
			i++
		}
		if i > 0 {
			id64, err := strconv.ParseInt(rest[:i], 10, 64)
			if err != nil || id64 < 0 || uint64(id64) > uint64(^uint(0)>>1) {
				return nil, invalid(string(raw), "ack id out of range")
			}
			id := int(id64)
			p.ID = &id
			rest = rest[i:]
		}
	}
	if rest != "" {
		p.Data = json.RawMessage(rest)
		if !json.Valid(p.Data) {
			return nil, invalid(string(raw), "invalid JSON data")
		}
	}
	if err := Validate(p); err != nil {
		return nil, invalid(string(raw), err.Error())
	}
	return p, nil
}

func Validate(p *Packet) error {
	if p == nil || !p.Type.Valid() {
		return errors.New("invalid packet type")
	}
	switch p.Type {
	case TypeConnect:
		if p.ID != nil {
			return errors.New("CONNECT cannot have an id")
		}
		if len(p.Data) > 0 && (p.Data[0] != '{' || !json.Valid(p.Data)) {
			return errors.New("CONNECT payload must be an object")
		}
	case TypeDisconnect:
		if p.ID != nil || len(p.Data) > 0 {
			return errors.New("DISCONNECT cannot have id or payload")
		}
	case TypeEvent, TypeBinaryEvent:
		if len(p.Data) == 0 {
			return errors.New("EVENT payload is required")
		}
		var a []json.RawMessage
		if err := json.Unmarshal(p.Data, &a); err != nil || len(a) == 0 {
			return errors.New("EVENT payload must be a non-empty array")
		}
	case TypeAck, TypeBinaryAck:
		if p.ID == nil {
			return errors.New("ACK requires an id")
		}
		if len(p.Data) == 0 {
			return errors.New("ACK payload is required")
		}
		var a []json.RawMessage
		if err := json.Unmarshal(p.Data, &a); err != nil {
			return errors.New("ACK payload must be an array")
		}
	case TypeConnectError:
		if p.ID != nil || len(p.Data) == 0 || p.Data[0] != '{' {
			return errors.New("CONNECT_ERROR payload must be an object")
		}
	}
	if !p.Type.IsBinary() && p.Attachments != 0 {
		return errors.New("unexpected attachments")
	}
	return nil
}

func EventName(data json.RawMessage) (string, error) {
	var a []json.RawMessage
	if err := json.Unmarshal(data, &a); err != nil || len(a) == 0 {
		return "", errors.New("packet: event data must be a non-empty JSON array")
	}
	var name string
	if err := json.Unmarshal(a[0], &name); err != nil || name == "" {
		return "", errors.New("packet: event name must be a non-empty string")
	}
	return name, nil
}

func EventArgs(data json.RawMessage) ([]json.RawMessage, error) {
	var a []json.RawMessage
	if err := json.Unmarshal(data, &a); err != nil || len(a) == 0 {
		return nil, errors.New("packet: event data must be a non-empty JSON array")
	}
	return a[1:], nil
}

func BuildEventData(event string, args ...interface{}) (json.RawMessage, error) {
	p := make([]interface{}, 0, len(args)+1)
	p = append(p, event)
	p = append(p, args...)
	b, err := json.Marshal(p)
	if err != nil {
		return nil, fmt.Errorf("packet: marshal event data: %w", err)
	}
	return json.RawMessage(b), nil
}

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

func BuildEventPacket(namespace, event string, id *int, args ...interface{}) (*Packet, [][]byte, error) {
	data, buffers, err := buildData(event, args...)
	if err != nil {
		return nil, nil, err
	}
	t := TypeEvent
	if len(buffers) > 0 {
		t = TypeBinaryEvent
	}
	p := &Packet{Type: t, Namespace: namespace, ID: id, Data: data, Attachments: len(buffers)}
	if _, err = Encode(p); err != nil {
		return nil, nil, err
	}
	return p, buffers, nil
}

func BuildAckPacket(namespace string, id *int, args ...interface{}) (*Packet, [][]byte, error) {
	data, buffers, err := buildArray(args...)
	if err != nil {
		return nil, nil, err
	}
	t := TypeAck
	if len(buffers) > 0 {
		t = TypeBinaryAck
	}
	p := &Packet{Type: t, Namespace: namespace, ID: id, Data: data, Attachments: len(buffers)}
	if _, err = Encode(p); err != nil {
		return nil, nil, err
	}
	return p, buffers, nil
}

func buildData(event string, args ...interface{}) (json.RawMessage, [][]byte, error) {
	v := make([]interface{}, 0, len(args)+1)
	v = append(v, event)
	v = append(v, args...)
	return marshalWithBinary(v)
}
func buildArray(args ...interface{}) (json.RawMessage, [][]byte, error) {
	return marshalWithBinary(args)
}

func marshalWithBinary(v interface{}) (json.RawMessage, [][]byte, error) {
	buffers := make([][]byte, 0)
	if !containsBinary(reflect.ValueOf(v)) {
		b, err := json.Marshal(v)
		return b, buffers, err
	}
	clean := deconstruct(reflect.ValueOf(v), &buffers)
	b, err := json.Marshal(clean)
	if err != nil {
		return nil, nil, err
	}
	return b, buffers, nil
}

func containsBinary(v reflect.Value) bool {
	if !v.IsValid() {
		return false
	}
	if v.Kind() == reflect.Interface || v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return false
		}
		return containsBinary(v.Elem())
	}
	if v.Kind() == reflect.Slice || v.Kind() == reflect.Array {
		if v.Type().Elem().Kind() == reflect.Uint8 {
			return true
		}
		for i := 0; i < v.Len(); i++ {
			if containsBinary(v.Index(i)) {
				return true
			}
		}
		return false
	}
	if v.Kind() == reflect.Map {
		for _, k := range v.MapKeys() {
			if containsBinary(v.MapIndex(k)) {
				return true
			}
		}
		return false
	}
	if v.Kind() == reflect.Struct {
		for i := 0; i < v.NumField(); i++ {
			if v.Type().Field(i).PkgPath != "" {
				continue
			}
			if containsBinary(v.Field(i)) {
				return true
			}
		}
		return false
	}
	return false
}

func deconstruct(v reflect.Value, buffers *[][]byte) interface{} {
	if !v.IsValid() {
		return nil
	}
	if v.Kind() == reflect.Interface || v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return nil
		}
		return deconstruct(v.Elem(), buffers)
	}
	if (v.Kind() == reflect.Slice || v.Kind() == reflect.Array) && v.Type().Elem().Kind() == reflect.Uint8 {
		b := make([]byte, v.Len())
		reflect.Copy(reflect.ValueOf(b), v)
		n := len(*buffers)
		*buffers = append(*buffers, b)
		return map[string]interface{}{"_placeholder": true, "num": n}
	}
	switch v.Kind() {
	case reflect.Slice, reflect.Array:
		a := make([]interface{}, v.Len())
		for i := 0; i < v.Len(); i++ {
			a[i] = deconstruct(v.Index(i), buffers)
		}
		return a
	case reflect.Map:
		m := map[string]interface{}{}
		for _, k := range v.MapKeys() {
			if k.Kind() != reflect.String {
				continue
			}
			m[k.String()] = deconstruct(v.MapIndex(k), buffers)
		}
		return m
	case reflect.Struct:
		m := map[string]interface{}{}
		t := v.Type()
		for i := 0; i < v.NumField(); i++ {
			sf := t.Field(i)
			if sf.PkgPath != "" {
				continue
			}
			tag := sf.Tag.Get("json")
			name := strings.Split(tag, ",")[0]
			if name == "-" {
				continue
			}
			if name == "" {
				name = sf.Name
			}
			m[name] = deconstruct(v.Field(i), buffers)
		}
		return m
	default:
		return v.Interface()
	}
}

func Reconstruct(data json.RawMessage, buffers [][]byte) (interface{}, error) {
	var v interface{}
	if err := json.Unmarshal(data, &v); err != nil {
		return nil, err
	}
	return reconstructValue(v, buffers)
}
func reconstructValue(v interface{}, buffers [][]byte) (interface{}, error) {
	switch x := v.(type) {
	case []interface{}:
		out := make([]interface{}, len(x))
		for i, e := range x {
			r, err := reconstructValue(e, buffers)
			if err != nil {
				return nil, err
			}
			out[i] = r
		}
		return out, nil
	case map[string]interface{}:
		if ph, ok := x["_placeholder"].(bool); ok && ph {
			n, ok := x["num"].(float64)
			if !ok || n < 0 || int(n) >= len(buffers) {
				return nil, errors.New("packet: invalid binary placeholder")
			}
			return append([]byte(nil), buffers[int(n)]...), nil
		}
		out := map[string]interface{}{}
		for k, e := range x {
			r, err := reconstructValue(e, buffers)
			if err != nil {
				return nil, err
			}
			out[k] = r
		}
		return out, nil
	default:
		return v, nil
	}
}

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
