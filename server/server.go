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

const (
	eioClose        = byte('1')
	eioPing         = byte('2')
	eioMessage      = byte('4')
	MaxRoomsPerConn = 100
)

type Conn interface {
	ID() string
	Namespace() string
	Emit(string, ...interface{}) error
	Join(string)
	Leave(string)
	Rooms() []string
	Context() interface{}
	SetContext(interface{})
	Close() error
}
type AckFunc func([]json.RawMessage, error)
type BinaryAckFunc func([]interface{}, error)
type EventHandler func(Conn, []json.RawMessage)
type BinaryEventHandler func(Conn, []interface{}, *int)
type ConnectHandler func(Conn) error
type DisconnectHandler func(Conn, string)
type ErrorHandler func(Conn, error)

type namespace struct {
	name         string
	rooms        *rooms.Manager
	mu           sync.RWMutex
	onConnect    ConnectHandler
	onDisconnect DisconnectHandler
	onError      ErrorHandler
	events       map[string]EventHandler
	binaryEvents map[string]BinaryEventHandler
}

func newNamespace(n string) *namespace {
	return &namespace{name: n, rooms: rooms.New(), events: make(map[string]EventHandler), binaryEvents: make(map[string]BinaryEventHandler)}
}
func (n *namespace) setConnect(f ConnectHandler) { n.mu.Lock(); n.onConnect = f; n.mu.Unlock() }
func (n *namespace) setDisconnect(f DisconnectHandler) {
	n.mu.Lock()
	n.onDisconnect = f
	n.mu.Unlock()
}
func (n *namespace) setError(f ErrorHandler)           { n.mu.Lock(); n.onError = f; n.mu.Unlock() }
func (n *namespace) setEvent(e string, f EventHandler) { n.mu.Lock(); n.events[e] = f; n.mu.Unlock() }
func (n *namespace) setBinaryEvent(e string, f BinaryEventHandler) {
	n.mu.Lock()
	n.binaryEvents[e] = f
	n.mu.Unlock()
}
func (n *namespace) getEvent(e string) (EventHandler, BinaryEventHandler) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.events[e], n.binaryEvents[e]
}

type outbound struct {
	text   []byte
	binary [][]byte
}
type engineSession struct {
	tr     transport.Conn
	mu     sync.Mutex
	closed atomic.Bool
}

func (s *engineSession) send(p *packet.Packet, buffers [][]byte) error {
	raw, err := packet.Encode(p)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed.Load() {
		return errors.New("gsocketio: session closed")
	}
	if err = s.tr.WriteText(append([]byte{eioMessage}, raw...)); err != nil {
		return err
	}
	if len(buffers) > 0 {
		bw, ok := s.tr.(interface{ WriteBinary([]byte) error })
		if !ok {
			return errors.New("gsocketio: transport does not support binary")
		}
		for _, b := range buffers {
			if err = bw.WriteBinary(b); err != nil {
				return err
			}
		}
	}
	return nil
}
func (s *engineSession) close() error { s.closed.Store(true); return s.tr.Close() }

type conn struct {
	id, ns     string
	session    *engineSession
	srv        *Server
	closed     atomic.Bool
	closeOnce  sync.Once
	ctxMu      sync.RWMutex
	ctx        interface{}
	roomMu     sync.RWMutex
	joined     map[string]struct{}
	ackSeq     atomic.Uint64
	ackMu      sync.Mutex
	acks       map[uint64]AckFunc
	binaryAcks map[uint64]BinaryAckFunc
}

func newConn(id, ns string, sess *engineSession, srv *Server) *conn {
	return &conn{id: id, ns: ns, session: sess, srv: srv, joined: make(map[string]struct{}), acks: make(map[uint64]AckFunc), binaryAcks: make(map[uint64]BinaryAckFunc)}
}
func (c *conn) ID() string               { return c.id }
func (c *conn) Namespace() string        { return c.ns }
func (c *conn) Context() interface{}     { c.ctxMu.RLock(); defer c.ctxMu.RUnlock(); return c.ctx }
func (c *conn) SetContext(v interface{}) { c.ctxMu.Lock(); c.ctx = v; c.ctxMu.Unlock() }
func (c *conn) Rooms() []string {
	c.roomMu.RLock()
	defer c.roomMu.RUnlock()
	out := make([]string, 0, len(c.joined))
	for r := range c.joined {
		out = append(out, r)
	}
	return out
}
func (c *conn) Join(room string) {
	if room == "" {
		return
	}
	c.roomMu.Lock()
	if _, ok := c.joined[room]; ok {
		c.roomMu.Unlock()
		return
	}
	if len(c.joined) >= MaxRoomsPerConn {
		c.roomMu.Unlock()
		return
	}
	c.joined[room] = struct{}{}
	c.roomMu.Unlock()
	if n := c.srv.namespace(c.ns); n != nil {
		n.rooms.Join(room, c)
	}
}
func (c *conn) Leave(room string) {
	c.roomMu.Lock()
	delete(c.joined, room)
	c.roomMu.Unlock()
	if n := c.srv.namespace(c.ns); n != nil {
		n.rooms.Leave(room, c)
	}
}
func (c *conn) leaveAll() {
	if n := c.srv.namespace(c.ns); n != nil {
		n.rooms.LeaveAll(c)
	}
	c.roomMu.Lock()
	c.joined = make(map[string]struct{})
	c.roomMu.Unlock()
}
func (c *conn) Emit(event string, args ...interface{}) error {
	p, b, err := packet.BuildEventPacket(c.ns, event, nil, args...)
	if err != nil {
		return fmt.Errorf("conn.Emit: %w", err)
	}
	return c.session.send(p, b)
}
func (c *conn) EmitWithAck(event string, fn AckFunc, args ...interface{}) error {
	data, buffers, err := packet.BuildEventPacket(c.ns, event, nil, args...)
	if err != nil {
		return err
	}
	seq := c.ackSeq.Add(1)
	if seq > uint64(^uint(0)>>1) {
		return errors.New("gsocketio: ack id overflow")
	}
	id := int(seq)
	c.ackMu.Lock()
	c.acks[seq] = fn
	c.ackMu.Unlock()
	data.ID = &id
	if err = c.session.send(data, buffers); err != nil {
		c.ackMu.Lock()
		delete(c.acks, seq)
		c.ackMu.Unlock()
		return err
	}
	return nil
}
func (c *conn) emitAck(id int, args ...interface{}) error {
	p, b, err := packet.BuildAckPacket(c.ns, &id, args...)
	if err != nil {
		return err
	}
	return c.session.send(p, b)
}
func (c *conn) fireAck(id int, p *packet.Packet, buffers [][]byte) {
	if p.Type.IsBinary() {
		v, err := packet.Reconstruct(p.Data, buffers)
		c.ackMu.Lock()
		fn := c.binaryAcks[uint64(id)]
		delete(c.binaryAcks, uint64(id))
		c.ackMu.Unlock()
		if fn != nil {
			if err != nil {
				fn(nil, err)
			} else if a, ok := v.([]interface{}); ok {
				fn(a, nil)
			} else {
				fn([]interface{}{v}, nil)
			}
		}
		return
	}
	args, err := packet.EventArgs(p.Data)
	c.ackMu.Lock()
	fn := c.acks[uint64(id)]
	delete(c.acks, uint64(id))
	c.ackMu.Unlock()
	if fn != nil {
		fn(args, err)
	}
}
func (c *conn) Close() error {
	var err error
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		c.leaveAll()
		p := &packet.Packet{Type: packet.TypeDisconnect, Namespace: c.ns}
		err = c.session.send(p, nil)
		c.srv.removeConn(c, "client disconnect")
	})
	return err
}

// EmitWithBinaryAck is the binary-capable variant of EmitWithAck.
func (c *conn) EmitWithBinaryAck(event string, fn BinaryAckFunc, args ...interface{}) error {
	p, b, err := packet.BuildEventPacket(c.ns, event, nil, args...)
	if err != nil {
		return err
	}
	seq := c.ackSeq.Add(1)
	id := int(seq)
	c.ackMu.Lock()
	c.binaryAcks[seq] = fn
	c.ackMu.Unlock()
	p.ID = &id
	if err = c.session.send(p, b); err != nil {
		c.ackMu.Lock()
		delete(c.binaryAcks, seq)
		c.ackMu.Unlock()
	}
	return err
}

type Server struct {
	tr         *transport.Server
	nsMu       sync.RWMutex
	namespaces map[string]*namespace
	connsMu    sync.RWMutex
	conns      map[string]*conn
	sessionMu  sync.Mutex
	sessions   map[*engineSession]map[string]*conn
	connCount  int64
	closed     atomic.Bool
}

func New(opts *transport.Options) *Server {
	return &Server{tr: transport.NewServer(opts), namespaces: make(map[string]*namespace), conns: make(map[string]*conn), sessions: make(map[*engineSession]map[string]*conn)}
}
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.tr.ServeHTTP(w, r) }
func (s *Server) Serve() error {
	for {
		tc, err := s.tr.Accept()
		if err != nil {
			return err
		}
		go s.handleSession(tc)
	}
}
func (s *Server) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	s.sessionMu.Lock()
	sessions := make([]*engineSession, 0, len(s.sessions))
	for sess := range s.sessions {
		sessions = append(sessions, sess)
	}
	s.sessionMu.Unlock()
	for _, sess := range sessions {
		s.teardownSession(sess, "server closed")
	}
	return s.tr.Close()
}
func (s *Server) Count() int                                  { return int(atomic.LoadInt64(&s.connCount)) }
func (s *Server) OnConnect(ns string, f ConnectHandler)       { s.ensureNamespace(ns).setConnect(f) }
func (s *Server) OnDisconnect(ns string, f DisconnectHandler) { s.ensureNamespace(ns).setDisconnect(f) }
func (s *Server) OnError(ns string, f ErrorHandler)           { s.ensureNamespace(ns).setError(f) }
func (s *Server) OnEvent(ns, e string, f EventHandler)        { s.ensureNamespace(ns).setEvent(e, f) }
func (s *Server) OnBinaryEvent(ns, e string, f BinaryEventHandler) {
	s.ensureNamespace(ns).setBinaryEvent(e, f)
}
func (s *Server) JoinRoom(ns, room string, c Conn) {
	if n := s.namespace(ns); n != nil {
		n.rooms.Join(room, c)
	}
}
func (s *Server) LeaveRoom(ns, room string, c Conn) {
	if n := s.namespace(ns); n != nil {
		n.rooms.Leave(room, c)
	}
}
func (s *Server) LeaveAllRooms(ns string, c Conn) {
	if n := s.namespace(ns); n != nil {
		n.rooms.LeaveAll(c)
	}
}
func (s *Server) ClearRoom(ns, room string) {
	if n := s.namespace(ns); n != nil {
		n.rooms.Clear(room)
	}
}
func (s *Server) RoomLen(ns, room string) int {
	if n := s.namespace(ns); n != nil {
		return n.rooms.Len(room)
	}
	return 0
}
func (s *Server) Rooms(ns string) []string {
	if n := s.namespace(ns); n != nil {
		return n.rooms.Names()
	}
	return nil
}
func (s *Server) RoomMembers(ns, room string) []Conn {
	n := s.namespace(ns)
	if n == nil {
		return nil
	}
	ms := n.rooms.Members(room)
	out := make([]Conn, 0, len(ms))
	for _, m := range ms {
		if c, ok := m.(Conn); ok {
			out = append(out, c)
		}
	}
	return out
}
func (s *Server) ForEachInRoom(ns, room string, f func(Conn)) {
	if n := s.namespace(ns); n != nil {
		n.rooms.ForEach(room, func(m rooms.Member) {
			if c, ok := m.(Conn); ok {
				f(c)
			}
		})
	}
}
func (s *Server) ToRoom(ns, room, event string, skip Conn, args ...interface{}) {
	if n := s.namespace(ns); n != nil {
		ids := map[string]struct{}{}
		if skip != nil {
			ids[skip.ID()] = struct{}{}
		}
		n.rooms.Send(room, event, ids, args...)
	}
}
func (s *Server) ToNamespace(ns, event string, args ...interface{}) {
	if n := s.namespace(ns); n != nil {
		n.rooms.SendAll(event, args...)
	}
}
func (s *Server) ensureNamespace(ns string) *namespace {
	ns = packet.NormalizeNS(ns)
	s.nsMu.Lock()
	defer s.nsMu.Unlock()
	if n := s.namespaces[ns]; n != nil {
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

func (s *Server) handleSession(tc transport.Conn) {
	sess := &engineSession{tr: tc}
	s.sessionMu.Lock()
	s.sessions[sess] = make(map[string]*conn)
	s.sessionMu.Unlock()
	defer s.teardownSession(sess, "transport closed")
	connectedAny := false
	var pending *binaryPending
	for {
		op, raw, err := tc.ReadMessage()
		if err != nil {
			return
		}
		if op == transport.OpBinary {
			if pending == nil {
				return
			}
			pending.buffers = append(pending.buffers, append([]byte(nil), raw...))
			if len(pending.buffers) == pending.pkt.Attachments {
				p := pending.pkt
				buffers := pending.buffers
				pending = nil
				s.dispatchPacket(sess, p, buffers)
			}
			continue
		}
		if len(raw) == 0 {
			continue
		}
		p, err := packet.Decode(raw)
		if err != nil {
			return
		}
		if p.Type.IsBinary() {
			if p.Attachments > 64 {
				return
			}
			pending = &binaryPending{pkt: p}
			continue
		}
		s.dispatchPacket(sess, p, nil)
		connectedAny = connectedAny || p.Type == packet.TypeConnect
		if !connectedAny && p.Type != packet.TypeConnect {
			return
		}
	}
}

type binaryPending struct {
	pkt     *packet.Packet
	buffers [][]byte
}

func (s *Server) dispatchPacket(sess *engineSession, p *packet.Packet, buffers [][]byte) {
	ns := p.Namespace
	n := s.namespace(ns)
	switch p.Type {
	case packet.TypeConnect:
		s.connectNamespace(sess, n, p)
	case packet.TypeDisconnect:
		s.disconnectNamespace(sess, ns, "client disconnect")
	case packet.TypeEvent, packet.TypeBinaryEvent:
		s.dispatchEvent(sess, n, p, buffers)
	case packet.TypeAck, packet.TypeBinaryAck:
		s.dispatchAck(sess, ns, p, buffers)
	default:
		s.sendConnectError(sess, ns, "invalid packet")
	}
}
func (s *Server) connectNamespace(sess *engineSession, n *namespace, p *packet.Packet) {
	ns := p.Namespace
	if n == nil {
		if ns == "/" {
			n = s.ensureNamespace(ns)
		} else {
			s.sendConnectError(sess, ns, "Invalid namespace")
			return
		}
	}
	s.sessionMu.Lock()
	if old := s.sessions[sess][ns]; old != nil {
		s.sessionMu.Unlock()
		return
	}
	c := newConn(transport.NewSID(), ns, sess, s)
	if len(p.Data) > 0 {
		var auth interface{}
		if err := json.Unmarshal(p.Data, &auth); err == nil {
			c.SetContext(auth)
		}
	}
	s.sessions[sess][ns] = c
	s.sessionMu.Unlock()
	s.connsMu.Lock()
	s.conns[c.id] = c
	s.connsMu.Unlock()
	atomic.AddInt64(&s.connCount, 1)
	reply := &packet.Packet{Type: packet.TypeConnect, Namespace: ns, Data: mustMarshal(map[string]string{"sid": c.id})}
	if err := sess.send(reply, nil); err != nil {
		_ = c.Close()
		return
	}
	n.mu.RLock()
	fn := n.onConnect
	n.mu.RUnlock()
	if fn != nil {
		if err := fn(c); err != nil {
			s.sendConnectError(sess, ns, "Not authorized")
			s.removeConn(c, "rejected")
			return
		}
	}
}
func (s *Server) sendConnectError(sess *engineSession, ns, msg string) {
	p := &packet.Packet{Type: packet.TypeConnectError, Namespace: ns, Data: mustMarshal(map[string]string{"message": msg})}
	_ = sess.send(p, nil)
}
func (s *Server) dispatchEvent(sess *engineSession, n *namespace, p *packet.Packet, buffers [][]byte) {
	s.sessionMu.Lock()
	c := s.sessions[sess][p.Namespace]
	s.sessionMu.Unlock()
	if c == nil {
		return
	}
	if p.Type == packet.TypeBinaryEvent {
		v, err := packet.Reconstruct(p.Data, buffers)
		if err != nil {
			s.reportError(c, err)
			return
		}
		name, err := packet.EventName(p.Data)
		if err != nil {
			s.reportError(c, err)
			return
		}
		_, bf := n.getEvent(name)
		if bf != nil {
			if a, ok := v.([]interface{}); ok {
				bf(c, a, p.ID)
			}
			return
		}
		return
	}
	name, err := packet.EventName(p.Data)
	if err != nil {
		s.reportError(c, err)
		return
	}
	args, err := packet.EventArgs(p.Data)
	if err != nil {
		s.reportError(c, err)
		return
	}
	fn, _ := n.getEvent(name)
	if fn != nil {
		go fn(c, args)
	}
	if p.ID != nil {
		go func() {
			if fn != nil {
				fn(c, args)
			}
			_ = c.emitAck(*p.ID)
		}()
	}
}
func (s *Server) dispatchAck(sess *engineSession, ns string, p *packet.Packet, buffers [][]byte) {
	s.sessionMu.Lock()
	c := s.sessions[sess][ns]
	s.sessionMu.Unlock()
	if c == nil || p.ID == nil {
		return
	}
	c.fireAck(*p.ID, p, buffers)
}
func (s *Server) reportError(c *conn, err error) {
	n := s.namespace(c.ns)
	if n == nil {
		return
	}
	n.mu.RLock()
	f := n.onError
	n.mu.RUnlock()
	if f != nil {
		f(c, err)
	}
}
func (s *Server) disconnectNamespace(sess *engineSession, ns, reason string) {
	s.sessionMu.Lock()
	c := s.sessions[sess][ns]
	if c != nil {
		delete(s.sessions[sess], ns)
	}
	s.sessionMu.Unlock()
	if c == nil {
		return
	}
	c.leaveAll()
	s.connsMu.Lock()
	delete(s.conns, c.id)
	s.connsMu.Unlock()
	atomic.AddInt64(&s.connCount, -1)
	if n := s.namespace(ns); n != nil {
		n.mu.RLock()
		f := n.onDisconnect
		n.mu.RUnlock()
		if f != nil {
			f(c, reason)
		}
	}
}
func (s *Server) removeConn(c *conn, reason string) {
	s.sessionMu.Lock()
	m := s.sessions[c.session]
	if m != nil {
		if m[c.ns] == c {
			delete(m, c.ns)
		}
	}
	s.sessionMu.Unlock()
	c.leaveAll()
	s.connsMu.Lock()
	delete(s.conns, c.id)
	s.connsMu.Unlock()
	atomic.AddInt64(&s.connCount, -1)
	if n := s.namespace(c.ns); n != nil {
		n.mu.RLock()
		f := n.onDisconnect
		n.mu.RUnlock()
		if f != nil {
			f(c, reason)
		}
	}
}
func (s *Server) teardownSession(sess *engineSession, reason string) {
	s.sessionMu.Lock()
	m := s.sessions[sess]
	delete(s.sessions, sess)
	s.sessionMu.Unlock()
	for _, c := range m {
		c.closed.Store(true)
		c.leaveAll()
		s.connsMu.Lock()
		delete(s.conns, c.id)
		s.connsMu.Unlock()
		atomic.AddInt64(&s.connCount, -1)
		if n := s.namespace(c.ns); n != nil {
			n.mu.RLock()
			f := n.onDisconnect
			n.mu.RUnlock()
			if f != nil {
				f(c, reason)
			}
		}
	}
	if !sess.closed.Swap(true) {
		_ = sess.tr.Close()
	}
}
func mustMarshal(v interface{}) json.RawMessage {
	b, e := json.Marshal(v)
	if e != nil {
		logger.Error("marshal error: %v", e)
		return json.RawMessage(`{}`)
	}
	return b
}
