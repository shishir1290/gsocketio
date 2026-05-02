// Package gsocketio is a Socket.IO v4 server for Go, built entirely from
// the Go standard library — no external dependencies whatsoever.
//
// # Quick start
//
//	srv := gsocketio.New(nil)
//
//	srv.OnConnect("/", func(c gsocketio.Conn) error {
//	    fmt.Println("connected:", c.ID())
//	    c.Join("lobby")
//	    return nil
//	})
//
//	srv.OnEvent("/", "chat", func(c gsocketio.Conn, args []json.RawMessage) {
//	    var msg string
//	    json.Unmarshal(args[0], &msg)
//	    srv.ToRoom("/", "lobby", "chat", nil, msg)
//	})
//
//	srv.OnDisconnect("/", func(c gsocketio.Conn, reason string) {
//	    fmt.Println("disconnected:", c.ID(), reason)
//	})
//
//	go srv.Serve()
//	http.Handle("/socket.io/", srv)
//	http.ListenAndServe(":8080", nil)
package gsocketio

import (
	"encoding/json"
	"net/http"

	"github.com/shishir1290/gsocketio/server"
	"github.com/shishir1290/gsocketio/transport"
)

// ─────────────────────────────────────────────────────────────────────────────
// Re-exported types so callers only import "github.com/shishir1290/gsocketio"
// ─────────────────────────────────────────────────────────────────────────────

// Conn is a live Socket.IO connection.
type Conn = server.Conn

// Options configures the underlying transport.
type Options = transport.Options

// EventHandler handles an incoming event.
type EventHandler = server.EventHandler

// ConnectHandler is called on client connect; return error to reject.
type ConnectHandler = server.ConnectHandler

// DisconnectHandler is called on client disconnect.
type DisconnectHandler = server.DisconnectHandler

// ErrorHandler is called on protocol errors.
type ErrorHandler = server.ErrorHandler

// AckFunc is called when the remote side acknowledges an event.
type AckFunc = server.AckFunc

// ─────────────────────────────────────────────────────────────────────────────
// Server
// ─────────────────────────────────────────────────────────────────────────────

// Server is the top-level Socket.IO server.
type Server struct {
	inner *server.Server
}

// New creates a new Socket.IO server.
// Pass nil opts to use sensible defaults (25 s ping, 1 MB max payload).
func New(opts *Options) *Server {
	return &Server{inner: server.New(opts)}
}

// ServeHTTP implements http.Handler. Mount this at "/socket.io/" on your mux.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.inner.ServeHTTP(w, r)
}

// Serve starts accepting connections (blocking). Call in a goroutine.
func (s *Server) Serve() error { return s.inner.Serve() }

// Close shuts down the server.
func (s *Server) Close() error { return s.inner.Close() }

// Count returns the number of live connections across all namespaces.
func (s *Server) Count() int { return s.inner.Count() }

// ── Event handlers ────────────────────────────────────────────────────────────

// OnConnect registers fn for client connections to namespace ns.
func (s *Server) OnConnect(ns string, fn ConnectHandler) {
	s.inner.OnConnect(ns, fn)
}

// OnDisconnect registers fn for client disconnections from namespace ns.
func (s *Server) OnDisconnect(ns string, fn DisconnectHandler) {
	s.inner.OnDisconnect(ns, fn)
}

// OnError registers fn for protocol errors in namespace ns.
func (s *Server) OnError(ns string, fn ErrorHandler) {
	s.inner.OnError(ns, fn)
}

// OnEvent registers fn to handle event in namespace ns.
// The fn signature is func(Conn, []json.RawMessage).
func (s *Server) OnEvent(ns, event string, fn EventHandler) {
	s.inner.OnEvent(ns, event, fn)
}

// ── Room management ───────────────────────────────────────────────────────────

// JoinRoom adds c to roomName inside ns.
func (s *Server) JoinRoom(ns, roomName string, c Conn) {
	s.inner.JoinRoom(ns, roomName, c)
}

// LeaveRoom removes c from roomName inside ns.
func (s *Server) LeaveRoom(ns, roomName string, c Conn) {
	s.inner.LeaveRoom(ns, roomName, c)
}

// LeaveAllRooms removes c from every room in ns.
func (s *Server) LeaveAllRooms(ns string, c Conn) {
	s.inner.LeaveAllRooms(ns, c)
}

// ClearRoom removes all members from roomName inside ns.
func (s *Server) ClearRoom(ns, roomName string) {
	s.inner.ClearRoom(ns, roomName)
}

// RoomLen returns the number of connections in roomName of ns.
func (s *Server) RoomLen(ns, roomName string) int {
	return s.inner.RoomLen(ns, roomName)
}

// Rooms returns all room names in ns.
func (s *Server) Rooms(ns string) []string {
	return s.inner.Rooms(ns)
}

// RoomMembers returns all Conn values in roomName of ns.
func (s *Server) RoomMembers(ns, roomName string) []Conn {
	return s.inner.RoomMembers(ns, roomName)
}

// ForEachInRoom calls fn for every connection in roomName of ns.
func (s *Server) ForEachInRoom(ns, roomName string, fn func(Conn)) {
	s.inner.ForEachInRoom(ns, roomName, fn)
}

// ── Broadcast ─────────────────────────────────────────────────────────────────

// ToRoom emits event+args to everyone in roomName of ns.
// Pass skip=nil to send to all members; pass a Conn to exclude that sender.
func (s *Server) ToRoom(ns, roomName, event string, skip Conn, args ...interface{}) {
	s.inner.ToRoom(ns, roomName, event, skip, args...)
}

// ToNamespace emits event+args to every connection in ns.
func (s *Server) ToNamespace(ns, event string, args ...interface{}) {
	s.inner.ToNamespace(ns, event, args...)
}

// ─────────────────────────────────────────────────────────────────────────────
// Package-level JSON helper (re-exported for user convenience)
// ─────────────────────────────────────────────────────────────────────────────

// Unmarshal is a convenience wrapper around json.Unmarshal.
func Unmarshal(data json.RawMessage, v interface{}) error {
	return json.Unmarshal(data, v)
}
