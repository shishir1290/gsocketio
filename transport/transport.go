// Package transport implements the WebSocket (RFC 6455) and HTTP long-poll
// transports from scratch, fully compatible with the Engine.IO v4 protocol
// used by socket.io-client (JavaScript/React/Next.js), flutter_socket_io,
// python-socketio, socket.io-client-java, and any standard WebSocket client.
//
// Cross-platform compatibility notes:
//   - EIO4 heartbeat loop: server sends "2" (ping), expects "3" (pong)
//   - If no pong arrives within PingTimeout, the connection is closed
//   - socket.io-client handles reconnect automatically on close
//   - All standard WebSocket libraries (ws, socket.io-client, websockets,
//     okhttp, starscream, etc.) work because we speak plain RFC 6455
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
// WebSocket opcodes (RFC 6455 §5.2)
// ─────────────────────────────────────────────────────────────────────────────

const (
	OpContinuation byte = 0x0
	OpText         byte = 0x1
	OpBinary       byte = 0x2
	OpClose        byte = 0x8
	OpPing         byte = 0x9
	OpPong         byte = 0xA
)

const wsGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

// WriteDeadline applied before every write — prevents goroutine leaks from slow clients.
const WriteDeadline = 10 * time.Second

// EIO packet bytes (Engine.IO v4 protocol)
const (
	eioOpen      = '0'
	eioClose     = '1'
	eioPing      = '2' // server → client heartbeat
	eioPong      = '3' // client → server heartbeat reply
	eioMessage   = '4' // wraps Socket.IO packets
	eioUpgrade   = '5'
	eioNoop      = '6'
)

var (
	ErrUnmaskedFrame  = errors.New("transport: client sent unmasked frame (RFC 6455 §5.1)")
	ErrPayloadTooLarge = errors.New("transport: payload exceeds MaxPayload limit")
	ErrPingTimeout    = errors.New("transport: ping timeout — client did not pong")
)

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

func wsAcceptKey(clientKey string) string {
	h := sha1.New()
	h.Write([]byte(clientKey + wsGUID))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

func isWebSocketUpgrade(r *http.Request) bool {
	return strings.EqualFold(r.Header.Get("Upgrade"), "websocket") &&
		strings.Contains(strings.ToLower(r.Header.Get("Connection")), "upgrade")
}

// NewSID generates a 22-character cryptographically random session ID.
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

type Options struct {
	PingInterval time.Duration // how often server pings client (default 25s)
	PingTimeout  time.Duration // how long to wait for pong before close (default 20s)
	MaxPayload   int           // max frame payload bytes (default 1MB)
}

func (o *Options) defaults() {
	if o.PingInterval == 0 { o.PingInterval = 25 * time.Second }
	if o.PingTimeout == 0  { o.PingTimeout  = 20 * time.Second }
	if o.MaxPayload == 0   { o.MaxPayload   = 1_000_000 }
}

// ─────────────────────────────────────────────────────────────────────────────
// WSConn — RFC 6455 WebSocket connection with EIO4 heartbeat
// ─────────────────────────────────────────────────────────────────────────────

// WSConn is a full-duplex WebSocket connection.
// It runs an internal heartbeat goroutine that sends EIO "2" pings and
// closes the connection if no "3" pong is received within PingTimeout.
// This is what makes Flutter, React Native, and all other clients stable.
type WSConn struct {
	raw        net.Conn
	rw         *bufio.ReadWriter
	mu         sync.Mutex
	closed     chan struct{}
	once       sync.Once
	maxPayload uint64

	// heartbeat
	pingInterval time.Duration
	pingTimeout  time.Duration
	pongCh       chan struct{} // receives signal when EIO pong arrives
}

func newWSConn(raw net.Conn, rw *bufio.ReadWriter, maxPayload int, opts Options) *WSConn {
	c := &WSConn{
		raw:          raw,
		rw:           rw,
		closed:       make(chan struct{}),
		maxPayload:   uint64(maxPayload),
		pingInterval: opts.PingInterval,
		pingTimeout:  opts.PingTimeout,
		pongCh:       make(chan struct{}, 1),
	}
	go c.heartbeatLoop()
	return c
}

func (c *WSConn) RemoteAddr() string     { return c.raw.RemoteAddr().String() }
func (c *WSConn) Done() <-chan struct{}   { return c.closed }

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

// WriteText sends a WebSocket text frame.
func (c *WSConn) WriteText(msg []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.raw.SetWriteDeadline(time.Now().Add(WriteDeadline)) //nolint:errcheck
	if err := writeFrame(c.rw.Writer, OpText, msg); err != nil {
		return err
	}
	return c.rw.Flush()
}

// heartbeatLoop sends EIO "2" pings every PingInterval.
// If the client doesn't reply with EIO "3" within PingTimeout, closes the connection.
// This is required for socket.io-client, flutter_socket_io, python-socketio, etc.
func (c *WSConn) heartbeatLoop() {
	ticker := time.NewTicker(c.pingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.closed:
			return
		case <-ticker.C:
			// Send EIO ping "2"
			if err := c.WriteText([]byte{eioPing}); err != nil {
				return
			}
			// Wait for EIO pong "3" within PingTimeout
			select {
			case <-c.pongCh:
				// good — client is alive
			case <-time.After(c.pingTimeout):
				c.Close() //nolint:errcheck
				return
			case <-c.closed:
				return
			}
		}
	}
}

// ReadMessage reads a complete WebSocket message.
// Handles EIO protocol bytes transparently:
//   - EIO "3" (pong) → signals heartbeat, returns next real message
//   - EIO "4" prefix  → strips it, returns the Socket.IO payload
// This is what allows socket.io-client to work without modification.
func (c *WSConn) ReadMessage() (byte, []byte, error) {
	var assembled []byte
	var msgOpcode byte

	for {
		fin, opcode, payload, err := readFrame(c.rw.Reader, c.maxPayload)
		if err != nil {
			if errors.Is(err, ErrUnmaskedFrame) {
				c.mu.Lock()
				c.raw.SetWriteDeadline(time.Now().Add(WriteDeadline)) //nolint:errcheck
				_ = writeFrame(c.rw.Writer, OpClose, []byte{0x03, 0xEA}) // 1002
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
			// WebSocket-level ping (not EIO ping) — auto-pong
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
				// Handle EIO protocol envelope transparently
				data, err := c.handleEIO(payload)
				if err != nil {
					// EIO pong or noop — loop back for next message
					continue
				}
				return opcode, data, nil
			}
			msgOpcode = opcode
			assembled = append(assembled[:0], payload...)
		}
	}
}

// handleEIO processes EIO4 envelope bytes.
// Returns the unwrapped payload, or error if the message was consumed internally.
func (c *WSConn) handleEIO(payload []byte) ([]byte, error) {
	if len(payload) == 0 {
		return payload, nil
	}
	switch payload[0] {
	case eioPong: // "3" — reply to our heartbeat ping
		select {
		case c.pongCh <- struct{}{}:
		default:
		}
		return nil, errors.New("eio pong consumed")

	case eioMessage: // "4" — Socket.IO packet, strip the "4" prefix
		return payload[1:], nil

	case eioPing: // "2" — client-initiated ping (some clients send this)
		// Reply with pong
		c.WriteText([]byte{eioPong}) //nolint:errcheck
		return nil, errors.New("eio ping consumed")

	case eioNoop: // "6"
		return nil, errors.New("eio noop consumed")

	default:
		// No EIO envelope (raw Socket.IO packet) — pass through as-is
		return payload, nil
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Frame I/O
// ─────────────────────────────────────────────────────────────────────────────

func readFrame(r *bufio.Reader, maxPayload uint64) (fin bool, opcode byte, payload []byte, err error) {
	b0, err := r.ReadByte()
	if err != nil { return false, 0, nil, err }
	fin = (b0 & 0x80) != 0
	opcode = b0 & 0x0F

	b1, err := r.ReadByte()
	if err != nil { return false, 0, nil, err }
	masked := (b1 & 0x80) != 0
	if !masked { return false, 0, nil, ErrUnmaskedFrame }

	payLen := uint64(b1 & 0x7F)
	switch payLen {
	case 126:
		var ext [2]byte
		if _, err = io.ReadFull(r, ext[:]); err != nil { return }
		payLen = uint64(binary.BigEndian.Uint16(ext[:]))
	case 127:
		var ext [8]byte
		if _, err = io.ReadFull(r, ext[:]); err != nil { return }
		payLen = binary.BigEndian.Uint64(ext[:])
	}

	if maxPayload > 0 && payLen > maxPayload {
		return false, 0, nil, ErrPayloadTooLarge
	}

	var maskKey [4]byte
	if _, err = io.ReadFull(r, maskKey[:]); err != nil { return }

	if payLen > 0 {
		payload = make([]byte, payLen)
		if _, err = io.ReadFull(r, payload); err != nil { return }
		for i := range payload { payload[i] ^= maskKey[i%4] }
	}
	return
}

func writeFrame(w *bufio.Writer, opcode byte, payload []byte) error {
	if err := w.WriteByte(0x80 | opcode); err != nil { return err }
	l := len(payload)
	switch {
	case l <= 125:
		if err := w.WriteByte(byte(l)); err != nil { return err }
	case l <= 65535:
		if err := w.WriteByte(126); err != nil { return err }
		var ext [2]byte
		binary.BigEndian.PutUint16(ext[:], uint16(l))
		if _, err := w.Write(ext[:]); err != nil { return err }
	default:
		if err := w.WriteByte(127); err != nil { return err }
		var ext [8]byte
		binary.BigEndian.PutUint64(ext[:], uint64(l))
		if _, err := w.Write(ext[:]); err != nil { return err }
	}
	if len(payload) > 0 {
		_, err := w.Write(payload)
		return err
	}
	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// PollConn — HTTP long-poll (fallback transport for restrictive networks)
// ─────────────────────────────────────────────────────────────────────────────

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

func (c *PollConn) ID() string          { return c.id }
func (c *PollConn) RemoteAddr() string  { return c.addr }
func (c *PollConn) Done() <-chan struct{} { return c.closed }

func (c *PollConn) WriteText(p []byte) error {
	select {
	case c.sendCh <- p:
		return nil
	case <-c.closed:
		return errors.New("poll: connection closed")
	}
}

func (c *PollConn) ReadMessage() (byte, []byte, error) {
	select {
	case p := <-c.recvCh:
		return OpText, p, nil
	case <-c.closed:
		return 0, nil, errors.New("poll: connection closed")
	}
}

func (c *PollConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Conn interface
// ─────────────────────────────────────────────────────────────────────────────

type Conn interface {
	WriteText([]byte) error
	ReadMessage() (opcode byte, payload []byte, err error)
	RemoteAddr() string
	Done() <-chan struct{}
	Close() error
}

// ─────────────────────────────────────────────────────────────────────────────
// Server — HTTP handler + connection acceptor
// ─────────────────────────────────────────────────────────────────────────────

type openPacket struct {
	SID          string   `json:"sid"`
	Upgrades     []string `json:"upgrades"`
	PingInterval int      `json:"pingInterval"`
	PingTimeout  int      `json:"pingTimeout"`
	MaxPayload   int      `json:"maxPayload"`
}

type Server struct {
	opts   Options
	connCh chan Conn

	mu     sync.RWMutex
	active map[string]Conn

	closed   chan struct{}
	once     sync.Once
	pollMu   sync.RWMutex
	pollSess map[string]*PollConn
}

func NewServer(opts *Options) *Server {
	o := Options{}
	if opts != nil { o = *opts }
	o.defaults()
	return &Server{
		opts:     o,
		connCh:   make(chan Conn, 64),
		active:   make(map[string]Conn),
		closed:   make(chan struct{}),
		pollSess: make(map[string]*PollConn),
	}
}

func (s *Server) Accept() (Conn, error) {
	select {
	case c := <-s.connCh:
		return c, nil
	case <-s.closed:
		return nil, errors.New("transport: server closed")
	}
}

func (s *Server) Remove(sid string) {
	s.mu.Lock()
	delete(s.active, sid)
	s.mu.Unlock()
}

func (s *Server) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.active)
}

func (s *Server) Close() error {
	s.once.Do(func() { close(s.closed) })
	return nil
}

// ServeHTTP is the HTTP entry point for all transports.
// Clients connect to /socket.io/?EIO=4&transport=websocket
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// CORS — required for browser clients (React, Next.js, Flutter Web)
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

func (s *Server) serveWebSocket(w http.ResponseWriter, r *http.Request) {
	// RFC 6455 §4.1: only version 13 is supported
	wsVersion := r.Header.Get("Sec-Websocket-Version")
	if wsVersion != "13" {
		w.Header().Set("Sec-WebSocket-Version", "13")
		http.Error(w, "unsupported WebSocket version; only 13 supported", http.StatusUpgradeRequired)
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
		return
	}

	// Send 101 Switching Protocols
	accept := wsAcceptKey(key)
	resp := "HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + accept + "\r\n\r\n"
	if _, err = io.WriteString(brw, resp); err != nil {
		raw.Close()
		return
	}
	if err = brw.Flush(); err != nil {
		raw.Close()
		return
	}

	sid := NewSID()
	wsConn := newWSConn(raw, brw, s.opts.MaxPayload, s.opts)

	// Send EIO open packet — socket.io-client expects this immediately
	op, _ := json.Marshal(openPacket{
		SID:          sid,
		Upgrades:     []string{},
		PingInterval: int(s.opts.PingInterval / time.Millisecond),
		PingTimeout:  int(s.opts.PingTimeout / time.Millisecond),
		MaxPayload:   s.opts.MaxPayload,
	})
	// EIO4 open = "0" + JSON
	if err = wsConn.WriteText(append([]byte{eioOpen}, op...)); err != nil {
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

var pollSessions sync.Map

func (s *Server) servePoll(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=UTF-8")
	sid := r.URL.Query().Get("sid")

	if r.Method == http.MethodPost {
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
		fmt.Fprintf(w, "%c%s", eioOpen, op)
		return
	}

	v, ok := pollSessions.Load(sid)
	if !ok {
		http.Error(w, "session not found", http.StatusBadRequest)
		return
	}
	pc := v.(*PollConn)
	timer := time.NewTimer(s.opts.PingInterval)
	defer timer.Stop()
	select {
	case payload := <-pc.sendCh:
		w.Write(payload) //nolint:errcheck
	case <-timer.C:
		fmt.Fprintf(w, "%c", eioPing)
	case <-pc.closed:
		fmt.Fprintf(w, "%c", eioClose)
	case <-r.Context().Done():
		pc.Close()
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Test helpers
// ─────────────────────────────────────────────────────────────────────────────

func NewWSConnForTest(raw net.Conn, rw *bufio.ReadWriter) *WSConn {
	return newWSConn(raw, rw, 1_000_000, Options{
		PingInterval: 25 * time.Second,
		PingTimeout:  20 * time.Second,
	})
}
