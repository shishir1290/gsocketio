// Package server is the core of gsocketio. It wires the transport, packet
// parser, and room manager together into a Socket.IO server.
//
// Design overview:
//
//	HTTP request
//	    ↓
//	transport.Server.ServeHTTP  ← WebSocket upgrade / long-poll
//	    ↓ transport.Conn
//	server.Server.handleConn   ← wait for first SIO CONNECT packet
//	    ↓
//	namespace.handleConn       ← fire OnConnect, start read/write pumps
//	    ↓
//	readPump                   ← decode packets, dispatch to event handlers
//	writePump                  ← drain sendCh → transport.Conn.WriteText
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

// ─────────────────────────────────────────────────────────────────────────────
// Conn — public interface
// ─────────────────────────────────────────────────────────────────────────────

// Conn is a single Socket.IO connection. It is safe to call from multiple
// goroutines.
type Conn interface {
	// ID returns the unique session identifier.
	ID() string
	// Namespace returns the Socket.IO namespace (always starts with "/").
	Namespace() string
	// Emit sends a named event with optional arguments to this client.
	Emit(event string, args ...interface{}) error
	// Join adds this connection to roomName.
	Join(roomName string)
	// Leave removes this connection from roomName.
	Leave(roomName string)
	// Rooms returns all rooms this connection has joined.
	Rooms() []string
	// Context returns the user-defined value stored on this connection.
	Context() interface{}
	// SetContext stores an arbitrary value on the connection.
	SetContext(v interface{})
	// Close disconnects the socket.
	Close() error
}

// ─────────────────────────────────────────────────────────────────────────────
// conn — internal implementation of Conn
// ─────────────────────────────────────────────────────────────────────────────

type conn struct {
	id        string
	ns        string
	tr        transport.Conn // underlying wire connection
	srv       *Server

	// send buffering
	sendCh chan []byte
	closed uint32     // 1 once Close() called (atomic)
	closeOnce sync.Once

	// user context
	ctxMu sync.RWMutex
	ctx   interface{}

	// room membership (mirrored locally for fast Rooms() query)
	roomMu sync.RWMutex
	joined map[string]struct{}

	// ack support
	ackMu  sync.Mutex
	ackSeq int
	acks   map[int]AckFunc
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
		acks:   make(map[int]AckFunc),
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

func (c *conn) Join(roomName string) {
	c.roomMu.Lock()
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

// Emit sends an EVENT packet to the client. It is non-blocking if the send
// buffer has space; otherwise it returns an error.
func (c *conn) Emit(event string, args ...interface{}) error {
	data, err := packet.BuildEventData(event, args...)
	if err != nil {
		return fmt.Errorf("conn.Emit: %w", err)
	}
	pkt := &packet.Packet{
		Type:      packet.TypeEvent,
		Namespace: c.ns,
		Data:      data,
	}
	raw, err := packet.Encode(pkt)
	if err != nil {
		return fmt.Errorf("conn.Emit encode: %w", err)
	}
	return c.enqueue(raw)
}

// EmitWithAck sends an EVENT packet and registers a callback for the ACK.
func (c *conn) EmitWithAck(event string, fn AckFunc, args ...interface{}) error {
	data, err := packet.BuildEventData(event, args...)
	if err != nil {
		return fmt.Errorf("conn.EmitWithAck: %w", err)
	}

	c.ackMu.Lock()
	id := c.ackSeq
	c.ackSeq++
	c.acks[id] = fn
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

// enqueue places raw bytes in the send channel.
func (c *conn) enqueue(raw []byte) error {
	if atomic.LoadUint32(&c.closed) == 1 {
		return errors.New("gsocketio: connection already closed")
	}
	select {
	case c.sendCh <- raw:
		return nil
	default:
		return errors.New("gsocketio: send buffer full")
	}
}

// fireAck invokes the registered ack callback for id.
func (c *conn) fireAck(id int, args []json.RawMessage, err error) {
	c.ackMu.Lock()
	fn, ok := c.acks[id]
	if ok {
		delete(c.acks, id)
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

// satisfy rooms.Member
func (c *conn) Emit2(event string, args ...interface{}) error {
	return c.Emit(event, args...)
}

// ─────────────────────────────────────────────────────────────────────────────
// namespace — per-namespace handler registry
// ─────────────────────────────────────────────────────────────────────────────

// EventHandler is the signature for user event handlers.
type EventHandler func(c Conn, args []json.RawMessage)

// ConnectHandler is called when a client connects to this namespace.
// Return a non-nil error to refuse the connection.
type ConnectHandler func(c Conn) error

// DisconnectHandler is called when a client disconnects.
type DisconnectHandler func(c Conn, reason string)

// ErrorHandler is called when a protocol or handler error occurs.
type ErrorHandler func(c Conn, err error)

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
	ns.mu.Lock()
	ns.onConnect = fn
	ns.mu.Unlock()
}

func (ns *namespace) setDisconnect(fn DisconnectHandler) {
	ns.mu.Lock()
	ns.onDisconnect = fn
	ns.mu.Unlock()
}

func (ns *namespace) setError(fn ErrorHandler) {
	ns.mu.Lock()
	ns.onError = fn
	ns.mu.Unlock()
}

func (ns *namespace) setEvent(event string, fn EventHandler) {
	ns.mu.Lock()
	ns.events[event] = fn
	ns.mu.Unlock()
}

func (ns *namespace) getEvent(event string) EventHandler {
	ns.mu.RLock()
	defer ns.mu.RUnlock()
	return ns.events[event]
}

// ─────────────────────────────────────────────────────────────────────────────
// Server
// ─────────────────────────────────────────────────────────────────────────────

// Server is the top-level Socket.IO server.
type Server struct {
	tr *transport.Server // underlying transport (WS / long-poll)

	nsMu       sync.RWMutex
	namespaces map[string]*namespace

	connsMu sync.RWMutex
	conns   map[string]*conn // sid → *conn
}

// New creates a new Socket.IO server.
// Pass nil for opts to use defaults (25 s ping interval, 1 MB max payload).
func New(opts *transport.Options) *Server {
	return &Server{
		tr:         transport.NewServer(opts),
		namespaces: make(map[string]*namespace),
		conns:      make(map[string]*conn),
	}
}

// ServeHTTP implements http.Handler — mount this on your HTTP mux.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.tr.ServeHTTP(w, r)
}

// Serve starts accepting transport connections. Call in a goroutine.
// It returns only when the server is closed.
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
func (s *Server) Close() error {
	return s.tr.Close()
}

// Count returns the number of active Socket.IO connections (all namespaces).
func (s *Server) Count() int {
	s.connsMu.RLock()
	defer s.connsMu.RUnlock()
	return len(s.conns)
}

// ── Namespace event registration ──────────────────────────────────────────────

// OnConnect registers fn to be called when a client connects to ns.
// Return a non-nil error from fn to reject the connection.
func (s *Server) OnConnect(ns string, fn ConnectHandler) {
	s.ensureNamespace(ns).setConnect(fn)
}

// OnDisconnect registers fn to be called when a client disconnects from ns.
func (s *Server) OnDisconnect(ns string, fn DisconnectHandler) {
	s.ensureNamespace(ns).setDisconnect(fn)
}

// OnError registers fn to be called when an error occurs in ns.
func (s *Server) OnError(ns string, fn ErrorHandler) {
	s.ensureNamespace(ns).setError(fn)
}

// OnEvent registers fn to handle event in namespace ns.
// fn receives the Conn and the raw JSON arguments after the event name.
func (s *Server) OnEvent(ns, event string, fn EventHandler) {
	s.ensureNamespace(ns).setEvent(event, fn)
}

// ── Room management ───────────────────────────────────────────────────────────

// JoinRoom adds c to roomName in namespace ns.
func (s *Server) JoinRoom(ns, roomName string, c Conn) {
	if n := s.namespace(ns); n != nil {
		n.rooms.Join(roomName, c.(rooms.Member))
	}
}

// LeaveRoom removes c from roomName in namespace ns.
func (s *Server) LeaveRoom(ns, roomName string, c Conn) {
	if n := s.namespace(ns); n != nil {
		n.rooms.Leave(roomName, c.(rooms.Member))
	}
}

// LeaveAllRooms removes c from all rooms in namespace ns.
func (s *Server) LeaveAllRooms(ns string, c Conn) {
	if n := s.namespace(ns); n != nil {
		n.rooms.LeaveAll(c.(rooms.Member))
	}
}

// ClearRoom removes all members from roomName in namespace ns.
func (s *Server) ClearRoom(ns, roomName string) {
	if n := s.namespace(ns); n != nil {
		n.rooms.Clear(roomName)
	}
}

// RoomLen returns the number of connections in roomName of namespace ns.
func (s *Server) RoomLen(ns, roomName string) int {
	if n := s.namespace(ns); n != nil {
		return n.rooms.Len(roomName)
	}
	return 0
}

// Rooms returns all room names for namespace ns.
func (s *Server) Rooms(ns string) []string {
	if n := s.namespace(ns); n != nil {
		return n.rooms.Names()
	}
	return nil
}

// RoomMembers returns all connections in roomName of namespace ns.
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

// ForEachInRoom calls fn for every connection in roomName of namespace ns.
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

// ToRoom emits event+args to every connection in roomName of ns,
// optionally skipping the sender (pass nil to skip nobody).
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

// ToNamespace emits event+args to every connection in ns.
func (s *Server) ToNamespace(ns, event string, args ...interface{}) {
	if n := s.namespace(ns); n != nil {
		n.rooms.SendAll(event, args...)
	}
}

// ── Internal helpers ──────────────────────────────────────────────────────────

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

// handleConn is started in a goroutine for each new transport connection.
func (s *Server) handleConn(tc transport.Conn) {
	// The first message on a new WS session is the EIO open packet (already
	// sent by transport). The first Socket.IO packet the client sends is a
	// CONNECT packet that tells us the requested namespace.
	_, raw, err := tc.ReadMessage()
	if err != nil {
		logger.Debug("handleConn: first read: %v", err)
		tc.Close()
		return
	}

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
		// Auto-register "/" on first connection if the user hasn't called OnConnect.
		if ns == "/" {
			n = s.ensureNamespace(ns)
		} else {
			logger.Warn("handleConn: namespace %q not registered — rejecting", ns)
			// Send a CONNECT_ERROR back to the client.
			errPkt := &packet.Packet{
				Type:      packet.TypeConnectError,
				Namespace: ns,
				Data:      mustMarshal(map[string]string{"message": "namespace not found"}),
			}
			if b, e2 := packet.Encode(errPkt); e2 == nil {
				tc.WriteText(b) //nolint:errcheck
			}
			tc.Close()
			return
		}
	}

	c := newConn(transport.NewSID(), ns, tc, s)

	// Register.
	s.connsMu.Lock()
	s.conns[c.id] = c
	s.connsMu.Unlock()

	// Send Socket.IO CONNECT ack.
	ackPkt := &packet.Packet{Type: packet.TypeConnect, Namespace: ns}
	if b, e2 := packet.Encode(ackPkt); e2 == nil {
		if err2 := tc.WriteText(b); err2 != nil {
			logger.Error("handleConn: send connect ack: %v", err2)
		}
	}

	// Fire user OnConnect handler.
	n.mu.RLock()
	connectFn := n.onConnect
	n.mu.RUnlock()
	if connectFn != nil {
		if err3 := connectFn(c); err3 != nil {
			logger.Info("handleConn: onConnect rejected: %v", err3)
			s.teardown(c, n, "rejected: "+err3.Error())
			return
		}
	}

	// Start write pump in background.
	go s.writePump(c)

	// Read pump (blocks until connection closes).
	s.readPump(c, n)
}

// readPump decodes incoming packets and dispatches events.
func (s *Server) readPump(c *conn, n *namespace) {
	defer s.teardown(c, n, "transport closed")

	for {
		_, raw, err := c.tr.ReadMessage()
		if err != nil {
			logger.Debug("readPump %s: %v", c.id, err)
			return
		}

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

// writePump drains c.sendCh and writes frames to the transport.
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

// dispatchEvent routes an EVENT packet to the registered handler.
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

	// If client wants an ack, send it back after the handler runs.
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

// dispatchAck fires the registered ack callback.
func (s *Server) dispatchAck(c *conn, pkt *packet.Packet) {
	if pkt.ID == nil {
		return
	}
	args, _ := packet.EventArgs(pkt.Data)
	c.fireAck(*pkt.ID, args, nil)
}

// teardown deregisters the connection and fires the OnDisconnect handler.
func (s *Server) teardown(c *conn, n *namespace, reason string) {
	c.Close() //nolint:errcheck

	s.connsMu.Lock()
	delete(s.conns, c.id)
	s.connsMu.Unlock()

	s.tr.Remove(c.id)

	n.mu.RLock()
	disconnFn := n.onDisconnect
	n.mu.RUnlock()
	if disconnFn != nil {
		disconnFn(c, reason)
	}
}

// mustMarshal is a helper for internal constant JSON payloads.
func mustMarshal(v interface{}) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic("gsocketio: mustMarshal: " + err.Error())
	}
	return b
}
