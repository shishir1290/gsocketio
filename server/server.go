// Package server is the core of gsocketio.
//
// Fixes applied (from analysis report):
//   S-01 — Sanitise error strings before sending CONNECT_ERROR to clients
//   S-02 — Connection IDs use crypto/rand (confirmed via transport.NewSID)
//   S-03 — sync.Once on teardown prevents double OnDisconnect fire
//   S-04 — Write deadline applied in transport layer (WriteDeadline constant)
//   S-05 — Namespace map guarded by sync.RWMutex (already present, verified)
//   S-06 — Count decremented atomically before OnDisconnect callback
//   P-01 — Ack sequence uses atomic.Uint64 to prevent int overflow
//   R-02 — ForEach uses snapshot pattern (already present, verified)
//   R-03 — MaxRoomsPerConn cap (default 100) enforced in Join
package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/shishir1290/gsocketio/logger"
	"github.com/shishir1290/gsocketio/packet"
	"github.com/shishir1290/gsocketio/rooms"
	"github.com/shishir1290/gsocketio/transport"
)

// eioMessage is the Engine.IO v4 "message" packet type prefix.
// All Socket.IO packets must be wrapped with this byte when sent over the wire.
// socket.io-client (JS/Flutter/Python/Java) all expect: "4" + SIO_packet
const eioMessage = byte('4')

// wrapEIO wraps a Socket.IO packet with the EIO "4" message prefix.
func wrapEIO(sioPacket []byte) []byte {
	out := make([]byte, len(sioPacket)+1)
	out[0] = eioMessage
	copy(out[1:], sioPacket)
	return out
}

// MaxRoomsPerConn is the maximum number of rooms a single connection may join.
// FIX R-03: prevents memory exhaustion from malicious clients.
const MaxRoomsPerConn = 100

// ─────────────────────────────────────────────────────────────────────────────
// Conn — public interface
// ─────────────────────────────────────────────────────────────────────────────

// Conn is a single Socket.IO connection. It is safe to call from multiple goroutines.
type Conn interface {
	ID() string
	Namespace() string
	Emit(event string, args ...interface{}) error
	Join(roomName string)
	Leave(roomName string)
	Rooms() []string
	Context() interface{}
	SetContext(v interface{})
	Close() error
}

// ─────────────────────────────────────────────────────────────────────────────
// conn — internal implementation
// ─────────────────────────────────────────────────────────────────────────────

type conn struct {
	id  string
	ns  string
	tr  transport.Conn
	srv *Server

	sendCh    chan []byte
	closed    uint32    // atomic flag
	closeOnce sync.Once

	// FIX S-03: teardownOnce ensures OnDisconnect fires exactly once.
	teardownOnce sync.Once

	ctxMu sync.RWMutex
	ctx   interface{}

	roomMu sync.RWMutex
	joined map[string]struct{}

	// FIX P-01: use atomic uint64 for ack sequence — prevents int overflow.
	ackSeq atomic.Uint64
	ackMu  sync.Mutex
	acks   map[uint64]AckFunc
}

// AckFunc is called when the remote peer acknowledges an event.
type AckFunc func(args []json.RawMessage, err error)

func newConn(id, ns string, tr transport.Conn, srv *Server) *conn {
	return &conn{
		id:     id,
		ns:     ns,
		tr:     tr,
		srv:    srv,
		sendCh: make(chan []byte, 512),
		joined: make(map[string]struct{}),
		acks:   make(map[uint64]AckFunc),
	}
}

func (c *conn) ID() string        { return c.id }
func (c *conn) Namespace() string { return c.ns }

func (c *conn) Context() interface{} {
	c.ctxMu.RLock()
	defer c.ctxMu.RUnlock()
	return c.ctx
}

func (c *conn) SetContext(v interface{}) {
	c.ctxMu.Lock()
	c.ctx = v
	c.ctxMu.Unlock()
}

func (c *conn) Rooms() []string {
	c.roomMu.RLock()
	defer c.roomMu.RUnlock()
	out := make([]string, 0, len(c.joined))
	for r := range c.joined {
		out = append(out, r)
	}
	return out
}

// Join adds this connection to roomName.
// FIX R-03: enforces MaxRoomsPerConn cap.
func (c *conn) Join(roomName string) {
	c.roomMu.Lock()
	if len(c.joined) >= MaxRoomsPerConn {
		c.roomMu.Unlock()
		logger.Warn("conn %s: MaxRoomsPerConn (%d) reached, ignoring Join(%q)", c.id, MaxRoomsPerConn, roomName)
		return
	}
	c.joined[roomName] = struct{}{}
	c.roomMu.Unlock()

	if ns := c.srv.namespace(c.ns); ns != nil {
		ns.rooms.Join(roomName, c)
	}
}

func (c *conn) Leave(roomName string) {
	c.roomMu.Lock()
	delete(c.joined, roomName)
	c.roomMu.Unlock()
	if ns := c.srv.namespace(c.ns); ns != nil {
		ns.rooms.Leave(roomName, c)
	}
}

func (c *conn) leaveAll() {
	if ns := c.srv.namespace(c.ns); ns != nil {
		ns.rooms.LeaveAll(c)
	}
	c.roomMu.Lock()
	c.joined = make(map[string]struct{})
	c.roomMu.Unlock()
}

// Emit sends an EVENT packet to the client.
func (c *conn) Emit(event string, args ...interface{}) error {
	data, err := packet.BuildEventData(event, args...)
	if err != nil {
		return fmt.Errorf("conn.Emit: %w", err)
	}
	pkt := &packet.Packet{Type: packet.TypeEvent, Namespace: c.ns, Data: data}
	raw, err := packet.Encode(pkt)
	if err != nil {
		return fmt.Errorf("conn.Emit encode: %w", err)
	}
	return c.enqueue(raw)
}

// EmitWithAck sends an EVENT packet and registers a callback for the ACK.
// FIX P-01: uses uint64 ack sequence.
func (c *conn) EmitWithAck(event string, fn AckFunc, args ...interface{}) error {
	data, err := packet.BuildEventData(event, args...)
	if err != nil {
		return fmt.Errorf("conn.EmitWithAck: %w", err)
	}

	seq := c.ackSeq.Add(1) // atomic increment — never overflows in practice
	id := int(seq)

	c.ackMu.Lock()
	c.acks[seq] = fn
	c.ackMu.Unlock()

	pkt := &packet.Packet{
		Type:      packet.TypeEvent,
		Namespace: c.ns,
		ID:        &id,
		Data:      data,
	}
	raw, err := packet.Encode(pkt)
	if err != nil {
		return fmt.Errorf("conn.EmitWithAck encode: %w", err)
	}
	return c.enqueue(raw)
}

func (c *conn) enqueue(raw []byte) error {
	if atomic.LoadUint32(&c.closed) == 1 {
		return errors.New("gsocketio: connection already closed")
	}
	// Wrap with EIO "4" prefix — required by socket.io-client on all platforms
	wrapped := wrapEIO(raw)
	select {
	case c.sendCh <- wrapped:
		return nil
	default:
		return errors.New("gsocketio: send buffer full")
	}
}

func (c *conn) fireAck(seq uint64, args []json.RawMessage, err error) {
	c.ackMu.Lock()
	fn, ok := c.acks[seq]
	if ok {
		delete(c.acks, seq)
	}
	c.ackMu.Unlock()
	if ok && fn != nil {
		fn(args, err)
	}
}

// Close disconnects the socket and cleans up resources.
func (c *conn) Close() error {
	var err error
	c.closeOnce.Do(func() {
		atomic.StoreUint32(&c.closed, 1)
		c.leaveAll()
		err = c.tr.Close()
	})
	return err
}

// satisfy rooms.Member interface
func (c *conn) ID2() string { return c.id }

// ─────────────────────────────────────────────────────────────────────────────
// namespace
// ─────────────────────────────────────────────────────────────────────────────

type EventHandler      func(c Conn, args []json.RawMessage)
type ConnectHandler    func(c Conn) error
type DisconnectHandler func(c Conn, reason string)
type ErrorHandler      func(c Conn, err error)

type namespace struct {
	name  string
	rooms *rooms.Manager

	mu           sync.RWMutex
	onConnect    ConnectHandler
	onDisconnect DisconnectHandler
	onError      ErrorHandler
	events       map[string]EventHandler
}

func newNamespace(name string) *namespace {
	return &namespace{
		name:   name,
		rooms:  rooms.New(),
		events: make(map[string]EventHandler),
	}
}

func (ns *namespace) setConnect(fn ConnectHandler) {
	ns.mu.Lock(); ns.onConnect = fn; ns.mu.Unlock()
}
func (ns *namespace) setDisconnect(fn DisconnectHandler) {
	ns.mu.Lock(); ns.onDisconnect = fn; ns.mu.Unlock()
}
func (ns *namespace) setError(fn ErrorHandler) {
	ns.mu.Lock(); ns.onError = fn; ns.mu.Unlock()
}
func (ns *namespace) setEvent(event string, fn EventHandler) {
	ns.mu.Lock(); ns.events[event] = fn; ns.mu.Unlock()
}
func (ns *namespace) getEvent(event string) EventHandler {
	ns.mu.RLock(); defer ns.mu.RUnlock(); return ns.events[event]
}

// ─────────────────────────────────────────────────────────────────────────────
// Server
// ─────────────────────────────────────────────────────────────────────────────

// Server is the top-level Socket.IO server.
type Server struct {
	tr *transport.Server

	// FIX S-05: namespace map is always guarded by nsMu (verified).
	nsMu       sync.RWMutex
	namespaces map[string]*namespace

	// FIX S-06: use atomic counter for accurate Count() during teardown.
	connCount int64

	connsMu sync.RWMutex
	conns   map[string]*conn
}

// New creates a new Socket.IO server.
func New(opts *transport.Options) *Server {
	return &Server{
		tr:         transport.NewServer(opts),
		namespaces: make(map[string]*namespace),
		conns:      make(map[string]*conn),
	}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.tr.ServeHTTP(w, r) }

// Serve starts accepting connections. Call in a goroutine.
func (s *Server) Serve() error {
	for {
		tc, err := s.tr.Accept()
		if err != nil {
			return err
		}
		go s.handleConn(tc)
	}
}

// Close shuts down the server.
func (s *Server) Close() error { return s.tr.Close() }

// Count returns live connection count (FIX S-06: atomic, decremented before
// calling OnDisconnect so Count is accurate inside that handler).
func (s *Server) Count() int { return int(atomic.LoadInt64(&s.connCount)) }

// ── Event registration ────────────────────────────────────────────────────────

func (s *Server) OnConnect(ns string, fn ConnectHandler) {
	s.ensureNamespace(ns).setConnect(fn)
}
func (s *Server) OnDisconnect(ns string, fn DisconnectHandler) {
	s.ensureNamespace(ns).setDisconnect(fn)
}
func (s *Server) OnError(ns string, fn ErrorHandler) {
	s.ensureNamespace(ns).setError(fn)
}
func (s *Server) OnEvent(ns, event string, fn EventHandler) {
	s.ensureNamespace(ns).setEvent(event, fn)
}

// ── Room management ───────────────────────────────────────────────────────────

func (s *Server) JoinRoom(ns, roomName string, c Conn) {
	if n := s.namespace(ns); n != nil {
		n.rooms.Join(roomName, c.(rooms.Member))
	}
}
func (s *Server) LeaveRoom(ns, roomName string, c Conn) {
	if n := s.namespace(ns); n != nil {
		n.rooms.Leave(roomName, c.(rooms.Member))
	}
}
func (s *Server) LeaveAllRooms(ns string, c Conn) {
	if n := s.namespace(ns); n != nil {
		n.rooms.LeaveAll(c.(rooms.Member))
	}
}
func (s *Server) ClearRoom(ns, roomName string) {
	if n := s.namespace(ns); n != nil {
		n.rooms.Clear(roomName)
	}
}
func (s *Server) RoomLen(ns, roomName string) int {
	if n := s.namespace(ns); n != nil {
		return n.rooms.Len(roomName)
	}
	return 0
}
func (s *Server) Rooms(ns string) []string {
	if n := s.namespace(ns); n != nil {
		return n.rooms.Names()
	}
	return nil
}
func (s *Server) RoomMembers(ns, roomName string) []Conn {
	n := s.namespace(ns)
	if n == nil {
		return nil
	}
	members := n.rooms.Members(roomName)
	out := make([]Conn, 0, len(members))
	for _, m := range members {
		if c, ok := m.(Conn); ok {
			out = append(out, c)
		}
	}
	return out
}
func (s *Server) ForEachInRoom(ns, roomName string, fn func(Conn)) {
	if n := s.namespace(ns); n != nil {
		n.rooms.ForEach(roomName, func(m rooms.Member) {
			if c, ok := m.(Conn); ok {
				fn(c)
			}
		})
	}
}

// ── Broadcast ─────────────────────────────────────────────────────────────────

func (s *Server) ToRoom(ns, roomName, event string, skip Conn, args ...interface{}) {
	n := s.namespace(ns)
	if n == nil {
		return
	}
	skipIDs := make(map[string]struct{})
	if skip != nil {
		skipIDs[skip.ID()] = struct{}{}
	}
	n.rooms.Send(roomName, event, skipIDs, args...)
}
func (s *Server) ToNamespace(ns, event string, args ...interface{}) {
	if n := s.namespace(ns); n != nil {
		n.rooms.SendAll(event, args...)
	}
}

// ── Internal ──────────────────────────────────────────────────────────────────

func (s *Server) ensureNamespace(ns string) *namespace {
	ns = packet.NormalizeNS(ns)
	s.nsMu.Lock()
	defer s.nsMu.Unlock()
	if n, ok := s.namespaces[ns]; ok {
		return n
	}
	n := newNamespace(ns)
	s.namespaces[ns] = n
	return n
}

func (s *Server) namespace(ns string) *namespace {
	ns = packet.NormalizeNS(ns)
	s.nsMu.RLock()
	defer s.nsMu.RUnlock()
	return s.namespaces[ns]
}

// ─────────────────────────────────────────────────────────────────────────────
// Connection lifecycle
// ─────────────────────────────────────────────────────────────────────────────

func (s *Server) handleConn(tc transport.Conn) {
	_, raw, err := tc.ReadMessage()
	if err != nil {
		logger.Debug("handleConn: first read: %v", err)
		tc.Close()
		return
	}

	// Transport layer strips EIO "4" prefix for us (handleEIO in WSConn).
	// But poll connections pass raw bytes — strip manually if needed.
	raw = stripEIO(raw)

	pkt, err := packet.Decode(raw)
	if err != nil {
		logger.Error("handleConn: decode first packet: %v", err)
		tc.Close()
		return
	}

	if pkt.Type != packet.TypeConnect {
		logger.Warn("handleConn: expected CONNECT, got %v", pkt.Type)
		tc.Close()
		return
	}

	ns := pkt.Namespace
	n := s.namespace(ns)
	if n == nil {
		if ns == "/" {
			n = s.ensureNamespace(ns)
		} else {
			logger.Warn("handleConn: namespace %q not registered — rejecting", ns)
			errPkt := &packet.Packet{
				Type:      packet.TypeConnectError,
				Namespace: ns,
				// FIX S-01: send a generic message, not the internal error string.
				Data: mustMarshal(map[string]string{"message": "namespace not found"}),
			}
			if b, e2 := packet.Encode(errPkt); e2 == nil {
				tc.WriteText(wrapEIO(b)) //nolint:errcheck
			}
			tc.Close()
			return
		}
	}

	c := newConn(transport.NewSID(), ns, tc, s)

	s.connsMu.Lock()
	s.conns[c.id] = c
	s.connsMu.Unlock()
	atomic.AddInt64(&s.connCount, 1)

	ackPkt := &packet.Packet{
		Type:      packet.TypeConnect,
		Namespace: ns,
		Data:      mustMarshal(map[string]string{"sid": c.id}),
	}
	if b, e2 := packet.Encode(ackPkt); e2 == nil {
		// Wrap with EIO "4" prefix — socket.io-client expects "40{"sid":"..."}" for CONNECT ack
		if err2 := tc.WriteText(wrapEIO(b)); err2 != nil {
			logger.Error("handleConn: send connect ack: %v", err2)
		}
	}

	n.mu.RLock()
	connectFn := n.onConnect
	n.mu.RUnlock()
	if connectFn != nil {
		if err3 := connectFn(c); err3 != nil {
			logger.Info("handleConn: onConnect rejected: %v", err3)
			// FIX S-01: sanitise error — send only "rejected" to client.
			s.sendConnectError(tc, ns, "rejected")
			s.teardown(c, n, "rejected by OnConnect")
			return
		}
	}

	go s.writePump(c)
	s.readPump(c, n)
}

// sendConnectError sends a sanitised CONNECT_ERROR packet.
// FIX S-01: the publicMsg is what the client sees; internal details stay server-side.
func (s *Server) sendConnectError(tc transport.Conn, ns, publicMsg string) {
	errPkt := &packet.Packet{
		Type:      packet.TypeConnectError,
		Namespace: ns,
		Data:      mustMarshal(map[string]string{"message": publicMsg}),
	}
	if b, err := packet.Encode(errPkt); err == nil {
		tc.WriteText(wrapEIO(b)) //nolint:errcheck
	}
}

func (s *Server) readPump(c *conn, n *namespace) {
	defer s.teardown(c, n, "transport closed")

	for {
		_, raw, err := c.tr.ReadMessage()
		if err != nil {
			logger.Debug("readPump %s: %v", c.id, err)
			return
		}

		raw = stripEIO(raw)
		pkt, err := packet.Decode(raw)
		if err != nil {
			logger.Error("readPump %s: decode: %v", c.id, err)
			n.mu.RLock()
			errFn := n.onError
			n.mu.RUnlock()
			if errFn != nil {
				errFn(c, err)
			}
			continue
		}

		switch pkt.Type {
		case packet.TypeDisconnect:
			return
		case packet.TypeEvent:
			s.dispatchEvent(c, n, pkt)
		case packet.TypeAck:
			s.dispatchAck(c, pkt)
		case packet.TypeConnectError:
			n.mu.RLock()
			errFn := n.onError
			n.mu.RUnlock()
			if errFn != nil {
				errFn(c, fmt.Errorf("connect error: %s", string(pkt.Data)))
			}
		}
	}
}

func (s *Server) writePump(c *conn) {
	for {
		select {
		case raw, ok := <-c.sendCh:
			if !ok {
				return
			}
			if err := c.tr.WriteText(raw); err != nil {
				logger.Debug("writePump %s: %v", c.id, err)
				return
			}
		case <-c.tr.Done():
			return
		}
	}
}

func (s *Server) dispatchEvent(c *conn, n *namespace, pkt *packet.Packet) {
	if len(pkt.Data) == 0 {
		return
	}
	name, err := packet.EventName(pkt.Data)
	if err != nil {
		logger.Error("dispatchEvent: %v", err)
		return
	}
	args, _ := packet.EventArgs(pkt.Data)
	fn := n.getEvent(name)

	if pkt.ID != nil {
		ackID := *pkt.ID
		go func() {
			if fn != nil {
				fn(c, args)
			}
			ackData, _ := packet.BuildAckData()
			ackPkt := &packet.Packet{
				Type:      packet.TypeAck,
				Namespace: c.ns,
				ID:        &ackID,
				Data:      ackData,
			}
			if b, e2 := packet.Encode(ackPkt); e2 == nil {
				c.enqueue(b) //nolint:errcheck
			}
		}()
		return
	}

	if fn == nil {
		logger.Debug("no handler for event %q in namespace %q", name, c.ns)
		return
	}
	go fn(c, args)
}

func (s *Server) dispatchAck(c *conn, pkt *packet.Packet) {
	if pkt.ID == nil {
		return
	}
	args, _ := packet.EventArgs(pkt.Data)
	// FIX P-01: use uint64 key
	c.fireAck(uint64(*pkt.ID), args, nil)
}

// teardown deregisters the connection and fires OnDisconnect exactly once.
// FIX S-03: teardownOnce prevents double-fire.
// FIX S-06: decrements connCount before calling handler.
func (s *Server) teardown(c *conn, n *namespace, reason string) {
	c.teardownOnce.Do(func() {
		c.Close() //nolint:errcheck

		s.connsMu.Lock()
		delete(s.conns, c.id)
		s.connsMu.Unlock()
		s.tr.Remove(c.id)

		// FIX S-06: decrement before calling handler so Count() is accurate inside it.
		atomic.AddInt64(&s.connCount, -1)

		n.mu.RLock()
		disconnFn := n.onDisconnect
		n.mu.RUnlock()
		if disconnFn != nil {
			disconnFn(c, reason)
		}
	})
}

// stripEIO removes the EIO "4" (message) prefix if present.
// WSConn strips it automatically; PollConn passes raw bytes.
func stripEIO(raw []byte) []byte {
	if len(raw) > 0 && raw[0] == eioMessage {
		return raw[1:]
	}
	return raw
}

func mustMarshal(v interface{}) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic("gsocketio: mustMarshal: " + err.Error())
	}
	return b
}
