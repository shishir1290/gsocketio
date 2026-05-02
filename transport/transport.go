// Package transport implements the WebSocket (RFC 6455) and HTTP long-poll
// transports from scratch using only the Go standard library.
//
// WebSocket wire protocol (RFC 6455):
//
//  Frame layout:
//   0                   1                   2                   3
//   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
//  +-+-+-+-+-------+-+-------------+-------------------------------+
//  |F|R|R|R| opcode|M| Payload len |    Extended payload length    |
//  |I|S|S|S|  (4)  |A|     (7)     |             (16/64)           |
//  |N|V|V|V|       |S|             |   (if payload len==126/127)   |
//  | |1|2|3|       |K|             |                               |
//  +-+-+-+-+-------+-+-------------+ - - - - - - - - - - - - - - -+
//  |     Extended payload length continued, if payload len == 127  |
//  + - - - - - - - - - - - - - - -+-------------------------------+
//  |                               |Masking-key, if MASK set to 1  |
//  +-------------------------------+-------------------------------+
//  | Masking-key (continued)       |          Payload Data         |
//  +-------------------------------- - - - - - - - - - - - - - - -+
//  :                     Payload Data continued ...                :
//  + - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - +
//  |                     Payload Data continued ...                |
//  +---------------------------------------------------------------+
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

// ─────────────────────────────────────────────────────────────────────────────
// WebSocket handshake
// ─────────────────────────────────────────────────────────────────────────────

// wsAcceptKey computes the Sec-WebSocket-Accept header value (RFC 6455 §4.2.2).
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
	raw    net.Conn
	rw     *bufio.ReadWriter
	mu     sync.Mutex // guards writes
	closed chan struct{}
	once   sync.Once
}

// newWSConn wraps a raw TCP connection that has already been HTTP-upgraded.
func newWSConn(raw net.Conn, rw *bufio.ReadWriter) *WSConn {
	return &WSConn{raw: raw, rw: rw, closed: make(chan struct{})}
}

// RemoteAddr returns the peer's network address.
func (c *WSConn) RemoteAddr() string { return c.raw.RemoteAddr().String() }

// Done returns a channel that is closed when the connection is closed.
func (c *WSConn) Done() <-chan struct{} { return c.closed }

// Close sends a Close frame and shuts down the underlying connection.
func (c *WSConn) Close() error {
	var err error
	c.once.Do(func() {
		// Best-effort close frame
		c.mu.Lock()
		_ = writeFrame(c.rw.Writer, OpClose, nil)
		_ = c.rw.Flush()
		c.mu.Unlock()
		err = c.raw.Close()
		close(c.closed)
	})
	return err
}

// WriteText sends a complete UTF-8 text message.
func (c *WSConn) WriteText(msg []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := writeFrame(c.rw.Writer, OpText, msg); err != nil {
		return err
	}
	return c.rw.Flush()
}

// ReadMessage reads a complete message (possibly assembled from continuation frames).
// Returns (opcode, payload, error). Auto-responds to Ping with Pong.
func (c *WSConn) ReadMessage() (byte, []byte, error) {
	var assembled []byte
	var msgOpcode byte

	for {
		fin, opcode, payload, err := readFrame(c.rw.Reader)
		if err != nil {
			return 0, nil, err
		}

		switch opcode {
		case OpClose:
			// Echo the close frame and return EOF.
			c.mu.Lock()
			_ = writeFrame(c.rw.Writer, OpClose, payload)
			_ = c.rw.Flush()
			c.mu.Unlock()
			return OpClose, payload, io.EOF

		case OpPing:
			// RFC 6455 §5.5.3: server must reply with Pong.
			c.mu.Lock()
			_ = writeFrame(c.rw.Writer, OpPong, payload)
			_ = c.rw.Flush()
			c.mu.Unlock()
			continue

		case OpPong:
			// Unsolicited pong — ignore.
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
			// Start of a fragmented message.
			msgOpcode = opcode
			assembled = append(assembled[:0], payload...)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Frame-level I/O (RFC 6455 §5)
// ─────────────────────────────────────────────────────────────────────────────

// readFrame reads one raw WebSocket frame from r.
// It handles masking (clients MUST mask; servers MUST NOT mask).
func readFrame(r *bufio.Reader) (fin bool, opcode byte, payload []byte, err error) {
	// Byte 0: FIN + RSV1-3 + opcode
	b0, err := r.ReadByte()
	if err != nil {
		return false, 0, nil, err
	}
	fin = (b0 & 0x80) != 0
	opcode = b0 & 0x0F

	// Byte 1: MASK + payload length (7 bits)
	b1, err := r.ReadByte()
	if err != nil {
		return false, 0, nil, err
	}
	masked := (b1 & 0x80) != 0
	payLen := uint64(b1 & 0x7F)

	// Extended payload length
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

	// Masking key (4 bytes, clients must set MASK=1)
	var maskKey [4]byte
	if masked {
		if _, err = io.ReadFull(r, maskKey[:]); err != nil {
			return
		}
	}

	// Payload
	if payLen > 0 {
		payload = make([]byte, payLen)
		if _, err = io.ReadFull(r, payload); err != nil {
			return
		}
		if masked {
			for i := range payload {
				payload[i] ^= maskKey[i%4]
			}
		}
	}
	return
}

// writeFrame writes a single server-side WebSocket frame (unmasked, FIN=1).
func writeFrame(w *bufio.Writer, opcode byte, payload []byte) error {
	// Byte 0: FIN=1 + opcode
	if err := w.WriteByte(0x80 | opcode); err != nil {
		return err
	}
	// Byte 1+: payload length (server side → MASK bit = 0)
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

// PollConn simulates a persistent connection over a series of HTTP GET/POST pairs.
type PollConn struct {
	id     string
	addr   string
	sendCh chan []byte // server → client
	recvCh chan []byte // client → server
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

// ID returns the session identifier.
func (c *PollConn) ID() string { return c.id }

// RemoteAddr returns the client address.
func (c *PollConn) RemoteAddr() string { return c.addr }

// Done returns a channel closed when the connection is torn down.
func (c *PollConn) Done() <-chan struct{} { return c.closed }

// WriteText queues a payload for the next GET poll.
func (c *PollConn) WriteText(p []byte) error {
	select {
	case c.sendCh <- p:
		return nil
	case <-c.closed:
		return errors.New("poll: connection closed")
	}
}

// ReadMessage blocks until the client POSTs data.
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

// Conn is the interface both WSConn and PollConn satisfy for the upper layer.
type Conn interface {
	// WriteText sends a text frame to the peer.
	WriteText([]byte) error
	// ReadMessage blocks until a message arrives.
	ReadMessage() (opcode byte, payload []byte, err error)
	// RemoteAddr returns the peer address string.
	RemoteAddr() string
	// Done returns a channel closed on teardown.
	Done() <-chan struct{}
	// Close shuts down the connection.
	Close() error
}

// ─────────────────────────────────────────────────────────────────────────────
// Session ID generator
// ─────────────────────────────────────────────────────────────────────────────

// NewSID generates a cryptographically random session identifier.
func NewSID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic("gsocketio: cannot read random bytes: " + err.Error())
	}
	return base64.RawURLEncoding.EncodeToString(b)
}

// ─────────────────────────────────────────────────────────────────────────────
// Server — HTTP handler that produces Conn values
// ─────────────────────────────────────────────────────────────────────────────

// Options configures the transport server.
type Options struct {
	// PingInterval is how often the server sends a heartbeat.
	PingInterval time.Duration
	// PingTimeout is how long to wait for a pong before closing.
	PingTimeout time.Duration
	// MaxPayload is the maximum allowed payload size in bytes.
	MaxPayload int
}

// defaults fills in zero-value Options.
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

// openPacket is what the server sends in the EIO handshake.
type openPacket struct {
	SID          string   `json:"sid"`
	Upgrades     []string `json:"upgrades"`
	PingInterval int      `json:"pingInterval"`
	PingTimeout  int      `json:"pingTimeout"`
	MaxPayload   int      `json:"maxPayload"`
}

// Server is an HTTP handler that performs the WebSocket/poll handshake and
// emits accepted Conn values on a channel.
type Server struct {
	opts   Options
	connCh chan Conn

	mu     sync.RWMutex
	active map[string]Conn // sid → Conn

	closed chan struct{}
	once   sync.Once

	// pollSessions stores *PollConn between HTTP requests (indexed by sid).
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

// Remove deletes a session from the active map.
func (s *Server) Remove(sid string) {
	s.mu.Lock()
	delete(s.active, sid)
	s.mu.Unlock()
}

// Count returns the number of active sessions.
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

// ServeHTTP is the HTTP entry point.
//
// Routing logic:
//   GET  ?transport=websocket  OR  Upgrade: websocket  → WebSocket upgrade
//   GET  ?transport=polling    (no sid)                → open new poll session
//   GET  ?transport=polling    (with sid)              → flush pending frames
//   POST ?transport=polling    (with sid)              → receive client data
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// CORS — allow any origin for development convenience.
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

// ─── WebSocket handler ────────────────────────────────────────────────────────

func (s *Server) serveWebSocket(w http.ResponseWriter, r *http.Request) {
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

	// Write 101 Switching Protocols response.
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
	wsConn := newWSConn(raw, brw)

	// Send EIO open packet ("0{...}").
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

// ─── Long-poll handler ────────────────────────────────────────────────────────

func (s *Server) servePoll(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=UTF-8")
	sid := r.URL.Query().Get("sid")

	if r.Method == http.MethodPost {
		// Client is uploading data.
		s.pollMu.RLock()
		pc, ok := s.pollSess[sid]
		s.pollMu.RUnlock()
		if ok {
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

	// GET — either open or flush.
	if sid == "" {
		// New session.
		sid = NewSID()
		pc := newPollConn(sid, r.RemoteAddr)

		s.pollMu.Lock()
		s.pollSess[sid] = pc
		s.pollMu.Unlock()

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

	// Flush pending frames.
	s.pollMu.RLock()
	pc, ok := s.pollSess[sid]
	s.pollMu.RUnlock()
	if !ok {
		http.Error(w, "session not found", http.StatusBadRequest)
		return
	}
	timer := time.NewTimer(s.opts.PingInterval)
	defer timer.Stop()
	select {
	case payload := <-pc.sendCh:
		w.Write(payload) //nolint:errcheck
	case <-timer.C:
		fmt.Fprint(w, "2") // heartbeat noop
	case <-pc.closed:
		fmt.Fprint(w, "1") // close signal
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Test helpers (exported so tests in other packages can use them)
// ─────────────────────────────────────────────────────────────────────────────

// NewWSConnForTest creates a WSConn from an existing raw connection and
// read-writer. Used only in tests.
func NewWSConnForTest(raw net.Conn, rw *bufio.ReadWriter) *WSConn {
	return newWSConn(raw, rw)
}
