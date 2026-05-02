// Package transport implements the WebSocket (RFC 6455) and HTTP long-poll
// transports from scratch using only the Go standard library.
//
// Fixes applied (from analysis report):
//   T-01 — Enforce client frame masking; close with 1002 if unmasked
//   T-02 — Enforce Sec-WebSocket-Version: 13; reject with HTTP 426 otherwise
//   T-03 — Enforce MaxPayload before allocating the payload buffer
//   T-04 — Propagate long-poll context cancellation errors
package transport

import (
	"bufio"
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// Constants — WebSocket opcodes (RFC 6455 §5.2)
// ─────────────────────────────────────────────────────────────────────────────

const (
	OpContinuation byte = 0x0
	OpText         byte = 0x1
	OpBinary       byte = 0x2
	OpClose        byte = 0x8
	OpPing         byte = 0x9
	OpPong         byte = 0xA
)

// wsGUID is the magic string defined in RFC 6455 §1.3.
const wsGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

// WriteDeadline is applied before every write to prevent goroutine leaks
// caused by slow/unresponsive clients (fixes S-04).
const WriteDeadline = 10 * time.Second

// ErrUnmaskedFrame is returned when a client sends an unmasked frame.
// RFC 6455 §5.1: server MUST close with status 1002 (protocol error).
var ErrUnmaskedFrame = errors.New("transport: client sent unmasked frame (RFC 6455 §5.1)")

// ErrPayloadTooLarge is returned when a frame exceeds MaxPayload.
var ErrPayloadTooLarge = errors.New("transport: payload exceeds MaxPayload limit")

// ─────────────────────────────────────────────────────────────────────────────
// WebSocket handshake helpers
// ─────────────────────────────────────────────────────────────────────────────

// wsAcceptKey computes Sec-WebSocket-Accept (RFC 6455 §4.2.2).
func wsAcceptKey(clientKey string) string {
	h := sha1.New()
	h.Write([]byte(clientKey + wsGUID))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

// isWebSocketUpgrade reports whether r is a WebSocket upgrade request.
func isWebSocketUpgrade(r *http.Request) bool {
	return strings.EqualFold(r.Header.Get("Upgrade"), "websocket") &&
		strings.Contains(strings.ToLower(r.Header.Get("Connection")), "upgrade")
}

// ─────────────────────────────────────────────────────────────────────────────
// WSConn — a single WebSocket connection
// ─────────────────────────────────────────────────────────────────────────────

// WSConn is a full-duplex WebSocket connection backed by a net.Conn.
type WSConn struct {
	raw        net.Conn
	rw         *bufio.ReadWriter
	mu         sync.Mutex // guards writes
	closed     chan struct{}
	once       sync.Once
	maxPayload uint64
}

// newWSConn wraps a raw TCP connection that has already been HTTP-upgraded.
func newWSConn(raw net.Conn, rw *bufio.ReadWriter, maxPayload int) *WSConn {
	return &WSConn{
		raw:        raw,
		rw:         rw,
		closed:     make(chan struct{}),
		maxPayload: uint64(maxPayload),
	}
}

// RemoteAddr returns the peer's network address.
func (c *WSConn) RemoteAddr() string { return c.raw.RemoteAddr().String() }

// Done returns a channel that is closed when the connection is closed.
func (c *WSConn) Done() <-chan struct{} { return c.closed }

// Close sends a Close frame and shuts down the underlying connection.
func (c *WSConn) Close() error {
	var err error
	c.once.Do(func() {
		c.mu.Lock()
		c.raw.SetWriteDeadline(time.Now().Add(WriteDeadline)) //nolint:errcheck
		_ = writeFrame(c.rw.Writer, OpClose, nil)
		_ = c.rw.Flush()
		c.mu.Unlock()
		err = c.raw.Close()
		close(c.closed)
	})
	return err
}

// WriteText sends a complete UTF-8 text message.
// FIX S-04: applies a write deadline before every write.
func (c *WSConn) WriteText(msg []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.raw.SetWriteDeadline(time.Now().Add(WriteDeadline)) //nolint:errcheck
	if err := writeFrame(c.rw.Writer, OpText, msg); err != nil {
		return err
	}
	return c.rw.Flush()
}

// ReadMessage reads a complete message, handling continuation frames.
// FIX T-01: rejects unmasked frames from clients with close code 1002.
func (c *WSConn) ReadMessage() (byte, []byte, error) {
	var assembled []byte
	var msgOpcode byte

	for {
		fin, opcode, payload, err := readFrame(c.rw.Reader, c.maxPayload)
		if err != nil {
			// FIX T-01: if unmasked frame, send close 1002 before returning
			if errors.Is(err, ErrUnmaskedFrame) {
				c.mu.Lock()
				c.raw.SetWriteDeadline(time.Now().Add(WriteDeadline)) //nolint:errcheck
				// Close status 1002 = protocol error (2 byte big-endian)
				_ = writeFrame(c.rw.Writer, OpClose, []byte{0x03, 0xEA})
				_ = c.rw.Flush()
				c.mu.Unlock()
			}
			return 0, nil, err
		}

		switch opcode {
		case OpClose:
			c.mu.Lock()
			c.raw.SetWriteDeadline(time.Now().Add(WriteDeadline)) //nolint:errcheck
			_ = writeFrame(c.rw.Writer, OpClose, payload)
			_ = c.rw.Flush()
			c.mu.Unlock()
			return OpClose, payload, io.EOF

		case OpPing:
			c.mu.Lock()
			c.raw.SetWriteDeadline(time.Now().Add(WriteDeadline)) //nolint:errcheck
			_ = writeFrame(c.rw.Writer, OpPong, payload)
			_ = c.rw.Flush()
			c.mu.Unlock()
			continue

		case OpPong:
			continue

		case OpContinuation:
			assembled = append(assembled, payload...)
			if fin {
				return msgOpcode, assembled, nil
			}

		default: // Text or Binary
			if fin {
				return opcode, payload, nil
			}
			msgOpcode = opcode
			assembled = append(assembled[:0], payload...)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Frame-level I/O (RFC 6455 §5)
// ─────────────────────────────────────────────────────────────────────────────

// readFrame reads one raw WebSocket frame.
// FIX T-01: returns ErrUnmaskedFrame if client did not set MASK bit.
// FIX T-03: rejects frames whose payload length exceeds maxPayload BEFORE
//
//	allocating memory.
func readFrame(r *bufio.Reader, maxPayload uint64) (fin bool, opcode byte, payload []byte, err error) {
	b0, err := r.ReadByte()
	if err != nil {
		return false, 0, nil, err
	}
	fin = (b0 & 0x80) != 0
	opcode = b0 & 0x0F

	b1, err := r.ReadByte()
	if err != nil {
		return false, 0, nil, err
	}
	masked := (b1 & 0x80) != 0

	// FIX T-01 — RFC 6455 §5.1: all client frames MUST be masked.
	if !masked {
		return false, 0, nil, ErrUnmaskedFrame
	}

	payLen := uint64(b1 & 0x7F)

	switch payLen {
	case 126:
		var ext [2]byte
		if _, err = io.ReadFull(r, ext[:]); err != nil {
			return
		}
		payLen = uint64(binary.BigEndian.Uint16(ext[:]))
	case 127:
		var ext [8]byte
		if _, err = io.ReadFull(r, ext[:]); err != nil {
			return
		}
		payLen = binary.BigEndian.Uint64(ext[:])
	}

	// FIX T-03 — check payload size BEFORE allocating memory.
	if maxPayload > 0 && payLen > maxPayload {
		return false, 0, nil, ErrPayloadTooLarge
	}

	var maskKey [4]byte
	if _, err = io.ReadFull(r, maskKey[:]); err != nil {
		return
	}

	if payLen > 0 {
		payload = make([]byte, payLen)
		if _, err = io.ReadFull(r, payload); err != nil {
			return
		}
		for i := range payload {
			payload[i] ^= maskKey[i%4]
		}
	}
	return
}

// writeFrame writes a single server-side WebSocket frame (unmasked, FIN=1).
func writeFrame(w *bufio.Writer, opcode byte, payload []byte) error {
	if err := w.WriteByte(0x80 | opcode); err != nil {
		return err
	}
	l := len(payload)
	switch {
	case l <= 125:
		if err := w.WriteByte(byte(l)); err != nil {
			return err
		}
	case l <= 65535:
		if err := w.WriteByte(126); err != nil {
			return err
		}
		var ext [2]byte
		binary.BigEndian.PutUint16(ext[:], uint16(l))
		if _, err := w.Write(ext[:]); err != nil {
			return err
		}
	default:
		if err := w.WriteByte(127); err != nil {
			return err
		}
		var ext [8]byte
		binary.BigEndian.PutUint64(ext[:], uint64(l))
		if _, err := w.Write(ext[:]); err != nil {
			return err
		}
	}
	if len(payload) > 0 {
		_, err := w.Write(payload)
		return err
	}
	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// PollConn — HTTP long-poll pseudo-connection
// ─────────────────────────────────────────────────────────────────────────────

// PollConn simulates a persistent connection over HTTP GET/POST pairs.
type PollConn struct {
	id     string
	addr   string
	sendCh chan []byte
	recvCh chan []byte
	closed chan struct{}
	once   sync.Once
}

func newPollConn(id, addr string) *PollConn {
	return &PollConn{
		id:     id,
		addr:   addr,
		sendCh: make(chan []byte, 128),
		recvCh: make(chan []byte, 128),
		closed: make(chan struct{}),
	}
}

func (c *PollConn) ID() string         { return c.id }
func (c *PollConn) RemoteAddr() string { return c.addr }
func (c *PollConn) Done() <-chan struct{} { return c.closed }

// WriteText queues a payload for the next GET poll.
// FIX T-04: respects context via select on closed channel.
func (c *PollConn) WriteText(p []byte) error {
	select {
	case c.sendCh <- p:
		return nil
	case <-c.closed:
		return errors.New("poll: connection closed")
	}
}

// ReadMessage blocks until the client POSTs data.
// FIX T-04: returns error immediately if connection is already closed.
func (c *PollConn) ReadMessage() (byte, []byte, error) {
	select {
	case p := <-c.recvCh:
		return OpText, p, nil
	case <-c.closed:
		return 0, nil, errors.New("poll: connection closed")
	}
}

// Close tears down the poll connection.
func (c *PollConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Conn — unified transport interface
// ─────────────────────────────────────────────────────────────────────────────

// Conn is the interface both WSConn and PollConn satisfy.
type Conn interface {
	WriteText([]byte) error
	ReadMessage() (opcode byte, payload []byte, err error)
	RemoteAddr() string
	Done() <-chan struct{}
	Close() error
}

// ─────────────────────────────────────────────────────────────────────────────
// Session ID generator — FIX S-02: always crypto/rand
// ─────────────────────────────────────────────────────────────────────────────

// NewSID generates a cryptographically random session identifier (22 chars).
func NewSID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic("gsocketio: crypto/rand unavailable: " + err.Error())
	}
	return base64.RawURLEncoding.EncodeToString(b)
}

// ─────────────────────────────────────────────────────────────────────────────
// Options
// ─────────────────────────────────────────────────────────────────────────────

// Options configures the transport server.
type Options struct {
	PingInterval time.Duration
	PingTimeout  time.Duration
	MaxPayload   int
}

func (o *Options) defaults() {
	if o.PingInterval == 0 {
		o.PingInterval = 25 * time.Second
	}
	if o.PingTimeout == 0 {
		o.PingTimeout = 20 * time.Second
	}
	if o.MaxPayload == 0 {
		o.MaxPayload = 1_000_000
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Server
// ─────────────────────────────────────────────────────────────────────────────

type openPacket struct {
	SID          string   `json:"sid"`
	Upgrades     []string `json:"upgrades"`
	PingInterval int      `json:"pingInterval"`
	PingTimeout  int      `json:"pingTimeout"`
	MaxPayload   int      `json:"maxPayload"`
}

// Server is an HTTP handler that upgrades connections and emits Conn values.
type Server struct {
	opts   Options
	connCh chan Conn

	mu     sync.RWMutex
	active map[string]Conn

	closed chan struct{}
	once   sync.Once

	pollMu   sync.RWMutex
	pollSess map[string]*PollConn
}

// NewServer creates a transport Server.
func NewServer(opts *Options) *Server {
	o := Options{}
	if opts != nil {
		o = *opts
	}
	o.defaults()
	return &Server{
		opts:     o,
		connCh:   make(chan Conn, 64),
		active:   make(map[string]Conn),
		closed:   make(chan struct{}),
		pollSess: make(map[string]*PollConn),
	}
}

// Accept blocks until a new transport connection is ready.
func (s *Server) Accept() (Conn, error) {
	select {
	case c := <-s.connCh:
		return c, nil
	case <-s.closed:
		return nil, errors.New("transport: server closed")
	}
}

// Remove deletes a session.
func (s *Server) Remove(sid string) {
	s.mu.Lock()
	delete(s.active, sid)
	s.mu.Unlock()
}

// Count returns active session count.
func (s *Server) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.active)
}

// Close shuts down the server.
func (s *Server) Close() error {
	s.once.Do(func() { close(s.closed) })
	return nil
}

// ServeHTTP dispatches HTTP requests to the correct transport handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	if isWebSocketUpgrade(r) || r.URL.Query().Get("transport") == "websocket" {
		s.serveWebSocket(w, r)
		return
	}
	s.servePoll(w, r)
}

// serveWebSocket performs the HTTP→WebSocket upgrade.
// FIX T-02: rejects Sec-WebSocket-Version != 13 with HTTP 426.
func (s *Server) serveWebSocket(w http.ResponseWriter, r *http.Request) {
	// FIX T-02 — RFC 6455 §4.1: only version 13 is supported.
	wsVersion := r.Header.Get("Sec-Websocket-Version")
	if wsVersion != "13" {
		w.Header().Set("Sec-WebSocket-Version", "13")
		http.Error(w,
			fmt.Sprintf("unsupported WebSocket version %q; only 13 is supported", wsVersion),
			http.StatusUpgradeRequired, // 426
		)
		return
	}

	key := r.Header.Get("Sec-Websocket-Key")
	if key == "" {
		http.Error(w, "missing Sec-WebSocket-Key", http.StatusBadRequest)
		return
	}

	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "hijack not supported", http.StatusInternalServerError)
		return
	}
	raw, brw, err := hj.Hijack()
	if err != nil {
		http.Error(w, "hijack failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	accept := wsAcceptKey(key)
	handshake := "HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + accept + "\r\n\r\n"
	if _, err = io.WriteString(brw, handshake); err != nil {
		raw.Close()
		return
	}
	if err = brw.Flush(); err != nil {
		raw.Close()
		return
	}

	sid := NewSID()
	wsConn := newWSConn(raw, brw, s.opts.MaxPayload)

	op, _ := json.Marshal(openPacket{
		SID:          sid,
		Upgrades:     []string{},
		PingInterval: int(s.opts.PingInterval / time.Millisecond),
		PingTimeout:  int(s.opts.PingTimeout / time.Millisecond),
		MaxPayload:   s.opts.MaxPayload,
	})
	if err = wsConn.WriteText(append([]byte("0"), op...)); err != nil {
		wsConn.Close()
		return
	}

	s.mu.Lock()
	s.active[sid] = wsConn
	s.mu.Unlock()

	select {
	case s.connCh <- wsConn:
	case <-s.closed:
		wsConn.Close()
	}
}

// pollSessions stores active long-poll sessions.
var pollSessions sync.Map

func (s *Server) servePoll(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=UTF-8")
	sid := r.URL.Query().Get("sid")

	if r.Method == http.MethodPost {
		// FIX T-04: check request context before processing.
		if r.Context().Err() != nil {
			http.Error(w, "request cancelled", http.StatusRequestTimeout)
			return
		}
		if v, ok := pollSessions.Load(sid); ok {
			pc := v.(*PollConn)
			buf := make([]byte, s.opts.MaxPayload)
			n, _ := r.Body.Read(buf)
			if n > 0 {
				select {
				case pc.recvCh <- buf[:n]:
				default:
				}
			}
		}
		fmt.Fprint(w, "ok")
		return
	}

	if sid == "" {
		sid = NewSID()
		pc := newPollConn(sid, r.RemoteAddr)
		pollSessions.Store(sid, pc)

		s.mu.Lock()
		s.active[sid] = pc
		s.mu.Unlock()

		select {
		case s.connCh <- pc:
		case <-s.closed:
			pc.Close()
			return
		}

		op, _ := json.Marshal(openPacket{
			SID:          sid,
			Upgrades:     []string{"websocket"},
			PingInterval: int(s.opts.PingInterval / time.Millisecond),
			PingTimeout:  int(s.opts.PingTimeout / time.Millisecond),
			MaxPayload:   s.opts.MaxPayload,
		})
		fmt.Fprintf(w, "0%s", op)
		return
	}

	v, ok := pollSessions.Load(sid)
	if !ok {
		http.Error(w, "session not found", http.StatusBadRequest)
		return
	}
	pc := v.(*PollConn)

	// FIX T-04: use request context to detect client disconnect mid-poll.
	timer := time.NewTimer(s.opts.PingInterval)
	defer timer.Stop()
	select {
	case payload := <-pc.sendCh:
		w.Write(payload) //nolint:errcheck
	case <-timer.C:
		fmt.Fprint(w, "2")
	case <-pc.closed:
		fmt.Fprint(w, "1")
	case <-r.Context().Done():
		// FIX T-04: client disconnected mid-long-poll — clean up gracefully.
		pc.Close()
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Test helpers
// ─────────────────────────────────────────────────────────────────────────────

// NewWSConnForTest creates a WSConn from an existing net.Conn and ReadWriter.
// Used only in tests.
func NewWSConnForTest(raw net.Conn, rw *bufio.ReadWriter) *WSConn {
	return newWSConn(raw, rw, 1_000_000)
}
