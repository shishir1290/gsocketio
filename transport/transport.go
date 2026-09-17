package transport

import (
	"bufio"
	"bytes"
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
	"sync/atomic"
	"time"
	"unicode/utf8"
)

const (
	OpContinuation byte = 0x0
	OpText         byte = 0x1
	OpBinary       byte = 0x2
	OpClose        byte = 0x8
	OpPing         byte = 0x9
	OpPong         byte = 0xA
)
const wsGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
const WriteDeadline = 10 * time.Second
const (
	eioOpen    = '0'
	eioClose   = '1'
	eioPing    = '2'
	eioPong    = '3'
	eioMessage = '4'
	eioUpgrade = '5'
	eioNoop    = '6'
)

var (
	ErrUnmaskedFrame   = errors.New("transport: client sent unmasked frame")
	ErrPayloadTooLarge = errors.New("transport: payload exceeds MaxPayload limit")
	ErrProtocol        = errors.New("transport: websocket protocol error")
	ErrPingTimeout     = errors.New("transport: engine.io ping timeout")
)

type Options struct {
	PingInterval time.Duration
	PingTimeout  time.Duration
	MaxPayload   int
}

func (o *Options) defaults() {
	if o.PingInterval <= 0 {
		o.PingInterval = 25 * time.Second
	}
	if o.PingTimeout <= 0 {
		o.PingTimeout = 20 * time.Second
	}
	if o.MaxPayload <= 0 {
		o.MaxPayload = 1_000_000
	}
}
func NewSID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return base64.RawURLEncoding.EncodeToString(b)
}
func wsAcceptKey(k string) string {
	h := sha1.New()
	_, _ = h.Write([]byte(k + wsGUID))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}
func isWebSocketUpgrade(r *http.Request) bool {
	return strings.EqualFold(r.Header.Get("Upgrade"), "websocket") && strings.Contains(strings.ToLower(r.Header.Get("Connection")), "upgrade")
}

type WSConn struct {
	raw                       net.Conn
	rw                        *bufio.ReadWriter
	mu                        sync.Mutex
	closed                    chan struct{}
	once                      sync.Once
	maxPayload                uint64
	pingInterval, pingTimeout time.Duration
	pongCh                    chan struct{}
	validateUTF8              bool
	heartbeat                 bool
}

func newWSConn(raw net.Conn, rw *bufio.ReadWriter, maxPayload int, opts Options) *WSConn {
	c := &WSConn{raw: raw, rw: rw, closed: make(chan struct{}), maxPayload: uint64(maxPayload), pingInterval: opts.PingInterval, pingTimeout: opts.PingTimeout, pongCh: make(chan struct{}, 1), validateUTF8: true, heartbeat: true}
	go c.heartbeatLoop()
	return c
}
func newProbeWSConn(raw net.Conn, rw *bufio.ReadWriter, maxPayload int, opts Options) *WSConn {
	c := newWSConn(raw, rw, maxPayload, opts)
	c.heartbeat = false
	return c
}
func (c *WSConn) RemoteAddr() string    { return c.raw.RemoteAddr().String() }
func (c *WSConn) Done() <-chan struct{} { return c.closed }
func (c *WSConn) Close() error {
	var err error
	c.once.Do(func() {
		c.mu.Lock()
		_ = c.raw.SetWriteDeadline(time.Now().Add(WriteDeadline))
		_ = writeFrame(c.rw.Writer, OpClose, []byte{0x03, 0xE8})
		_ = c.rw.Flush()
		c.mu.Unlock()
		err = c.raw.Close()
		close(c.closed)
	})
	return err
}
func (c *WSConn) WriteText(p []byte) error {
	if len(p) > int(c.maxPayload) && c.maxPayload > 0 {
		return ErrPayloadTooLarge
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.raw.SetWriteDeadline(time.Now().Add(WriteDeadline)); err != nil {
		return err
	}
	if err := writeFrame(c.rw.Writer, OpText, p); err != nil {
		return err
	}
	return c.rw.Flush()
}
func (c *WSConn) WriteBinary(p []byte) error {
	if len(p) > int(c.maxPayload) && c.maxPayload > 0 {
		return ErrPayloadTooLarge
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.raw.SetWriteDeadline(time.Now().Add(WriteDeadline)); err != nil {
		return err
	}
	if err := writeFrame(c.rw.Writer, OpBinary, p); err != nil {
		return err
	}
	return c.rw.Flush()
}
func (c *WSConn) heartbeatLoop() {
	if !c.heartbeat {
		return
	}
	ticker := time.NewTicker(c.pingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.closed:
			return
		case <-ticker.C:
			if err := c.WriteText([]byte{eioPing}); err != nil {
				return
			}
			timer := time.NewTimer(c.pingTimeout)
			select {
			case <-c.closed:
				timer.Stop()
				return
			case <-c.pongCh:
				timer.Stop()
			case <-timer.C:
				_ = c.Close()
				return
			}
		}
	}
}
func (c *WSConn) signalPong() {
	select {
	case c.pongCh <- struct{}{}:
	default:
	}
}
func (c *WSConn) ReadMessage() (byte, []byte, error) {
	var msg []byte
	var opcode byte
	fragmented := false
	for {
		fin, op, p, err := readFrame(c.rw.Reader, c.maxPayload)
		if err != nil {
			if errors.Is(err, ErrUnmaskedFrame) || errors.Is(err, ErrProtocol) {
				c.sendCloseCode(1002)
			}
			return 0, nil, err
		}
		if op == OpPing {
			c.sendControl(OpPong, p)
			continue
		}
		if op == OpPong {
			c.signalPong()
			continue
		}
		if op == OpClose {
			c.sendControl(OpClose, p)
			return OpClose, p, io.EOF
		}
		if op == OpContinuation {
			if !fragmented {
				c.sendCloseCode(1002)
				return 0, nil, ErrProtocol
			}
			if len(msg)+len(p) > int(c.maxPayload) && c.maxPayload > 0 {
				return 0, nil, ErrPayloadTooLarge
			}
			msg = append(msg, p...)
			if fin {
				fragmented = false
				if opcode == OpText && c.validateUTF8 && !utf8.Valid(msg) {
					c.sendCloseCode(1007)
					return opcode, msg, ErrProtocol
				}
				hop, hmsg, herr := c.handleEIO(opcode, msg)
				if herr != nil {
					return hop, hmsg, herr
				}
				if len(hmsg) == 0 && hop != OpBinary {
					continue
				}
				return hop, hmsg, nil
			}
			continue
		}
		if op != OpText && op != OpBinary {
			c.sendCloseCode(1002)
			return 0, nil, ErrProtocol
		}
		if fragmented {
			c.sendCloseCode(1002)
			return 0, nil, ErrProtocol
		}
		if fin {
			if op == OpText && c.validateUTF8 && !utf8.Valid(p) {
				c.sendCloseCode(1007)
				return op, p, ErrProtocol
			}
			hop, hmsg, herr := c.handleEIO(op, p)
			if herr != nil {
				return hop, hmsg, herr
			}
			if len(hmsg) == 0 && hop != OpBinary {
				continue
			}
			return hop, hmsg, nil
		}
		fragmented = true
		opcode = op
		msg = append(msg[:0], p...)
	}
}
func (c *WSConn) handleEIO(op byte, p []byte) (byte, []byte, error) {
	if op == OpBinary {
		return op, p, nil
	}
	if len(p) == 0 {
		return op, p, nil
	}
	switch p[0] {
	case eioPong:
		c.signalPong()
		return c.handleEIO(OpText, p[1:])
	case eioMessage:
		return OpText, p[1:], nil
	case eioPing:
		if len(p) > 1 && string(p[1:]) == "probe" {
			return OpText, p, nil
		}
		_ = c.WriteText(append([]byte{eioPong}, p[1:]...))
		return OpText, nil, nil
	case eioClose:
		return OpClose, p, io.EOF
	case eioNoop:
		return OpText, nil, nil
	default:
		return OpText, p, nil
	}
}
func (c *WSConn) sendControl(op byte, p []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	_ = c.raw.SetWriteDeadline(time.Now().Add(WriteDeadline))
	_ = writeFrame(c.rw.Writer, op, p)
	_ = c.rw.Flush()
}
func (c *WSConn) sendCloseCode(code uint16) {
	var p [2]byte
	binary.BigEndian.PutUint16(p[:], code)
	c.mu.Lock()
	_ = c.raw.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
	_ = writeFrame(c.rw.Writer, OpClose, p[:])
	_ = c.rw.Flush()
	c.mu.Unlock()
	_ = c.raw.Close()
}
func readFrame(r *bufio.Reader, maxPayload uint64) (bool, byte, []byte, error) {
	b0, err := r.ReadByte()
	if err != nil {
		return false, 0, nil, err
	}
	if b0&0x70 != 0 {
		return false, 0, nil, ErrProtocol
	}
	fin := b0&0x80 != 0
	op := b0 & 0x0f
	if (op >= 3 && op <= 7) || op >= 11 {
		return false, 0, nil, ErrProtocol
	}
	b1, err := r.ReadByte()
	if err != nil {
		return false, 0, nil, err
	}
	if b1&0x80 == 0 {
		return false, 0, nil, ErrUnmaskedFrame
	}
	n := uint64(b1 & 0x7f)
	switch n {
	case 126:
		var x [2]byte
		if _, err = io.ReadFull(r, x[:]); err != nil {
			return false, 0, nil, err
		}
		n = uint64(binary.BigEndian.Uint16(x[:]))
	case 127:
		var x [8]byte
		if _, err = io.ReadFull(r, x[:]); err != nil {
			return false, 0, nil, err
		}
		if x[0]&0x80 != 0 {
			return false, 0, nil, ErrProtocol
		}
		n = binary.BigEndian.Uint64(x[:])
	}
	if op >= 8 && (!fin || n > 125) {
		return false, 0, nil, ErrProtocol
	}
	if maxPayload > 0 && n > maxPayload {
		return false, 0, nil, ErrPayloadTooLarge
	}
	var mask [4]byte
	if _, err = io.ReadFull(r, mask[:]); err != nil {
		return false, 0, nil, err
	}
	p := make([]byte, n)
	if n > 0 {
		if _, err = io.ReadFull(r, p); err != nil {
			return false, 0, nil, err
		}
		for i := range p {
			p[i] ^= mask[i%4]
		}
	}
	return fin, op, p, nil
}
func writeFrame(w *bufio.Writer, op byte, p []byte) error {
	if op >= 8 && (!(op == OpClose || op == OpPing || op == OpPong) || len(p) > 125) {
		return ErrProtocol
	}
	if err := w.WriteByte(0x80 | op); err != nil {
		return err
	}
	n := len(p)
	switch {
	case n <= 125:
		if err := w.WriteByte(byte(n)); err != nil {
			return err
		}
	case n <= 65535:
		if err := w.WriteByte(126); err != nil {
			return err
		}
		var x [2]byte
		binary.BigEndian.PutUint16(x[:], uint16(n))
		if _, err := w.Write(x[:]); err != nil {
			return err
		}
	default:
		if err := w.WriteByte(127); err != nil {
			return err
		}
		var x [8]byte
		binary.BigEndian.PutUint64(x[:], uint64(n))
		if _, err := w.Write(x[:]); err != nil {
			return err
		}
	}
	_, err := w.Write(p)
	return err
}

type PollConn struct {
	id, addr                  string
	sendCh, recvCh            chan []byte
	closed                    chan struct{}
	once                      sync.Once
	pongCh                    chan struct{}
	pingInterval, pingTimeout time.Duration
	getInFlight, postInFlight atomic.Int32
}

func newPollConn(id, addr string, opts Options) *PollConn {
	c := &PollConn{id: id, addr: addr, sendCh: make(chan []byte, 256), recvCh: make(chan []byte, 256), closed: make(chan struct{}), pongCh: make(chan struct{}, 1), pingInterval: opts.PingInterval, pingTimeout: opts.PingTimeout}
	go c.heartbeatLoop()
	return c
}
func (c *PollConn) ID() string            { return c.id }
func (c *PollConn) RemoteAddr() string    { return c.addr }
func (c *PollConn) Done() <-chan struct{} { return c.closed }
func (c *PollConn) WriteText(p []byte) error {
	select {
	case c.sendCh <- append([]byte(nil), p...):
		return nil
	case <-c.closed:
		return errors.New("poll: connection closed")
	}
}
func (c *PollConn) WriteBinary(p []byte) error {
	enc := base64.StdEncoding.EncodeToString(p)
	return c.WriteText(append([]byte{'b'}, enc...))
}
func (c *PollConn) ReadMessage() (byte, []byte, error) {
	for {
		select {
		case p := <-c.recvCh:
			if len(p) == 0 {
				continue
			}
			if p[0] == 'b' {
				b, err := base64.StdEncoding.DecodeString(string(p[1:]))
				if err != nil {
					return 0, nil, ErrProtocol
				}
				return OpBinary, b, nil
			}
			switch p[0] {
			case eioPong:
				c.signalPong()
				continue
			case eioPing:
				_ = c.WriteText([]byte{eioPong})
				continue
			case eioMessage:
				return OpText, p[1:], nil
			case eioClose:
				return OpClose, p, io.EOF
			case eioNoop:
				continue
			default:
				return OpText, p, nil
			}
		case <-c.closed:
			return 0, nil, errors.New("poll: connection closed")
		}
	}
}
func (c *PollConn) signalPong() {
	select {
	case c.pongCh <- struct{}{}:
	default:
	}
}
func (c *PollConn) heartbeatLoop() {
	ticker := time.NewTicker(c.pingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.closed:
			return
		case <-ticker.C:
			if err := c.WriteText([]byte{eioPing}); err != nil {
				return
			}
			timer := time.NewTimer(c.pingTimeout)
			select {
			case <-c.closed:
				timer.Stop()
				return
			case <-c.pongCh:
				timer.Stop()
			case <-timer.C:
				_ = c.Close()
				return
			}
		}
	}
}
func (c *PollConn) Close() error { c.once.Do(func() { close(c.closed) }); return nil }

type Conn interface {
	WriteText([]byte) error
	ReadMessage() (byte, []byte, error)
	RemoteAddr() string
	Done() <-chan struct{}
	Close() error
}
type binaryWriter interface{ WriteBinary([]byte) error }

type sessionConn struct {
	mu      sync.RWMutex
	current Conn
	done    chan struct{}
	closed  atomic.Bool
}

func newSessionConn(c Conn) *sessionConn { return &sessionConn{current: c, done: make(chan struct{})} }
func (s *sessionConn) RemoteAddr() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.current.RemoteAddr()
}
func (s *sessionConn) Done() <-chan struct{} { return s.done }
func (s *sessionConn) WriteText(p []byte) error {
	s.mu.RLock()
	c := s.current
	s.mu.RUnlock()
	return c.WriteText(p)
}
func (s *sessionConn) WriteBinary(p []byte) error {
	s.mu.RLock()
	c := s.current
	s.mu.RUnlock()
	if b, ok := c.(binaryWriter); ok {
		return b.WriteBinary(p)
	}
	return errors.New("transport: binary unsupported")
}
func (s *sessionConn) ReadMessage() (byte, []byte, error) {
	for {
		s.mu.RLock()
		c := s.current
		s.mu.RUnlock()
		op, p, err := c.ReadMessage()
		if err == nil {
			return op, p, nil
		}
		s.mu.RLock()
		changed := s.current != c
		s.mu.RUnlock()
		if changed {
			continue
		}
		return op, p, err
	}
}
func (s *sessionConn) Close() error {
	s.mu.Lock()
	c := s.current
	s.mu.Unlock()
	err := c.Close()
	if s.closed.CompareAndSwap(false, true) {
		close(s.done)
	}
	return err
}
func (s *sessionConn) upgrade(c *WSConn) {
	s.mu.Lock()
	old := s.current
	s.current = c
	s.mu.Unlock()
	_ = old.Close()
}

type openPacket struct {
	SID          string   `json:"sid"`
	Upgrades     []string `json:"upgrades"`
	PingInterval int      `json:"pingInterval"`
	PingTimeout  int      `json:"pingTimeout"`
	MaxPayload   int      `json:"maxPayload"`
}
type Server struct {
	opts     Options
	connCh   chan Conn
	mu       sync.RWMutex
	active   map[string]*sessionConn
	closed   chan struct{}
	once     sync.Once
	pollMu   sync.RWMutex
	pollSess map[string]*PollConn
}

func NewServer(opts *Options) *Server {
	o := Options{}
	if opts != nil {
		o = *opts
	}
	o.defaults()
	return &Server{opts: o, connCh: make(chan Conn, 64), active: make(map[string]*sessionConn), closed: make(chan struct{}), pollSess: make(map[string]*PollConn)}
}
func (s *Server) Accept() (Conn, error) {
	select {
	case c := <-s.connCh:
		return c, nil
	case <-s.closed:
		return nil, errors.New("transport: server closed")
	}
}
func (s *Server) Remove(id string) {
	s.mu.Lock()
	delete(s.active, id)
	s.mu.Unlock()
	s.pollMu.Lock()
	if p := s.pollSess[id]; p != nil {
		_ = p.Close()
	}
	delete(s.pollSess, id)
	s.pollMu.Unlock()
}
func (s *Server) RemoveConn(c Conn) {
	s.mu.Lock()
	for id, v := range s.active {
		if v == c {
			delete(s.active, id)
			break
		}
	}
	s.mu.Unlock()
}
func (s *Server) Count() int { s.mu.RLock(); defer s.mu.RUnlock(); return len(s.active) }
func (s *Server) Close() error {
	s.once.Do(func() {
		close(s.closed)
		s.mu.Lock()
		cs := make([]*sessionConn, 0, len(s.active))
		for _, c := range s.active {
			cs = append(cs, c)
		}
		s.active = make(map[string]*sessionConn)
		s.mu.Unlock()
		for _, c := range cs {
			_ = c.Close()
		}
		s.pollMu.Lock()
		s.pollSess = make(map[string]*PollConn)
		s.pollMu.Unlock()
	})
	return nil
}
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	q := r.URL.Query()
	if q.Get("EIO") != "4" {
		http.Error(w, "unsupported Engine.IO version", 400)
		return
	}
	switch q.Get("transport") {
	case "websocket":
		if !isWebSocketUpgrade(r) {
			http.Error(w, "websocket upgrade required", 400)
			return
		}
		s.serveWebSocket(w, r)
	case "polling":
		s.servePoll(w, r)
	default:
		http.Error(w, "unsupported transport", 400)
	}
}
func (s *Server) serveWebSocket(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Sec-WebSocket-Version") != "13" {
		w.Header().Set("Sec-WebSocket-Version", "13")
		http.Error(w, "unsupported WebSocket version", 426)
		return
	}
	key := r.Header.Get("Sec-WebSocket-Key")
	if key == "" {
		http.Error(w, "missing Sec-WebSocket-Key", 400)
		return
	}
	sid := r.URL.Query().Get("sid")
	if sid != "" {
		s.mu.RLock()
		sess, ok := s.active[sid]
		s.mu.RUnlock()
		if !ok {
			http.Error(w, "session not found", 400)
			return
		}
		s.pollMu.RLock()
		pc := s.pollSess[sid]
		s.pollMu.RUnlock()
		if pc == nil {
			http.Error(w, "session already upgraded", 400)
			return
		}
		_ = pc.getInFlight.Load() // no-op: pending GET is woken by noop
		hj, ok := w.(http.Hijacker)
		if !ok {
			http.Error(w, "hijack not supported", 500)
			return
		}
		raw, rw, err := hj.Hijack()
		if err != nil {
			return
		}
		resp := "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " + wsAcceptKey(key) + "\r\n\r\n"
		if _, err = io.WriteString(rw, resp); err != nil {
			_ = raw.Close()
			return
		}
		if err = rw.Flush(); err != nil {
			_ = raw.Close()
			return
		}
		ws := newProbeWSConn(raw, rw, s.opts.MaxPayload, s.opts)
		_ = pc.WriteText([]byte{eioNoop})
		for {
			_, p, err := ws.ReadMessage()
			if err != nil {
				_ = ws.Close()
				return
			}
			if bytes.Equal(p, []byte{eioPing, 'p', 'r', 'o', 'b', 'e'}) {
				if err = ws.WriteText([]byte{eioPong, 'p', 'r', 'o', 'b', 'e'}); err != nil {
					_ = ws.Close()
					return
				}
				continue
			}
			if bytes.Equal(p, []byte{eioUpgrade}) {
				s.mu.Lock()
				sess.upgrade(ws)
				s.mu.Unlock()
				s.pollMu.Lock()
				delete(s.pollSess, sid)
				s.pollMu.Unlock()
				return
			}
			_ = ws.Close()
			return
		}
	}
	raw, rw, err := func() (net.Conn, *bufio.ReadWriter, error) {
		hj, ok := w.(http.Hijacker)
		if !ok {
			http.Error(w, "hijack not supported", 500)
			return nil, nil, errors.New("no hijacker")
		}
		return hj.Hijack()
	}()
	if err != nil {
		return
	}
	resp := "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " + wsAcceptKey(key) + "\r\n\r\n"
	if _, err = io.WriteString(rw, resp); err != nil {
		_ = raw.Close()
		return
	}
	if err = rw.Flush(); err != nil {
		_ = raw.Close()
		return
	}
	ws := newWSConn(raw, rw, s.opts.MaxPayload, s.opts)
	sid = NewSID()
	op, _ := json.Marshal(openPacket{SID: sid, Upgrades: []string{}, PingInterval: int(s.opts.PingInterval / time.Millisecond), PingTimeout: int(s.opts.PingTimeout / time.Millisecond), MaxPayload: s.opts.MaxPayload})
	if err = ws.WriteText(append([]byte{eioOpen}, op...)); err != nil {
		_ = ws.Close()
		return
	}
	sess := newSessionConn(ws)
	s.mu.Lock()
	s.active[sid] = sess
	s.mu.Unlock()
	select {
	case s.connCh <- sess:
	case <-s.closed:
		_ = sess.Close()
		s.Remove(sid)
	}
}
func (s *Server) servePoll(w http.ResponseWriter, r *http.Request) {
	sid := r.URL.Query().Get("sid")
	w.Header().Set("Content-Type", "text/plain; charset=UTF-8")
	if r.Method == http.MethodPost {
		if sid == "" {
			http.Error(w, "missing sid", 400)
			return
		}
		s.pollMu.RLock()
		pc, ok := s.pollSess[sid]
		s.pollMu.RUnlock()
		if !ok {
			http.Error(w, "session not found", 400)
			return
		}
		if !pc.postInFlight.CompareAndSwap(0, 1) {
			_ = pc.Close()
			http.Error(w, "concurrent POST", 400)
			return
		}
		defer pc.postInFlight.Store(0)
		body, err := io.ReadAll(io.LimitReader(r.Body, int64(s.opts.MaxPayload)+1))
		if err != nil {
			http.Error(w, "bad request", 400)
			return
		}
		if len(body) > s.opts.MaxPayload {
			http.Error(w, "payload too large", 413)
			return
		}
		for _, p := range bytes.Split(body, []byte{0x1e}) {
			if len(p) == 0 {
				continue
			}
			select {
			case pc.recvCh <- append([]byte(nil), p...):
			case <-pc.closed:
				http.Error(w, "session closed", 400)
				return
			}
		}
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "ok")
		return
	}
	if sid == "" {
		sid = NewSID()
		pc := newPollConn(sid, r.RemoteAddr, s.opts)
		sess := newSessionConn(pc)
		s.pollMu.Lock()
		s.pollSess[sid] = pc
		s.pollMu.Unlock()
		s.mu.Lock()
		s.active[sid] = sess
		s.mu.Unlock()
		op, _ := json.Marshal(openPacket{SID: sid, Upgrades: []string{"websocket"}, PingInterval: int(s.opts.PingInterval / time.Millisecond), PingTimeout: int(s.opts.PingTimeout / time.Millisecond), MaxPayload: s.opts.MaxPayload})
		_, _ = fmt.Fprintf(w, "%c%s", eioOpen, op)
		select {
		case s.connCh <- sess:
		case <-s.closed:
			_ = sess.Close()
			s.Remove(sid)
		}
		return
	}
	s.pollMu.RLock()
	pc, ok := s.pollSess[sid]
	s.pollMu.RUnlock()
	if !ok {
		http.Error(w, "session not found", 400)
		return
	}
	if !pc.getInFlight.CompareAndSwap(0, 1) {
		_ = pc.Close()
		http.Error(w, "concurrent GET", 400)
		return
	}
	defer pc.getInFlight.Store(0)
	select {
	case p := <-pc.sendCh:
		_, _ = w.Write(p)
	case <-pc.closed:
		http.Error(w, "session closed", 400)
	case <-r.Context().Done():
		return
	}
}
func NewWSConnForTest(raw net.Conn, rw *bufio.ReadWriter) *WSConn {
	c := newWSConn(raw, rw, 1_000_000, Options{PingInterval: 25 * time.Second, PingTimeout: 20 * time.Second})
	c.validateUTF8 = false
	return c
}
