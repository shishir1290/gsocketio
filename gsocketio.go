// Package gsocketio is a Socket.IO v4 server for Go, built entirely from
the Go standard library — no external dependencies whatsoever.
package gsocketio

import (
	"encoding/json"
	"net/http"

	"github.com/shishir1290/gsocketio/server"
	"github.com/shishir1290/gsocketio/transport"
)

type Conn = server.Conn
type Options = transport.Options
type EventHandler = server.EventHandler
type BinaryEventHandler = server.BinaryEventHandler
type ConnectHandler = server.ConnectHandler
type DisconnectHandler = server.DisconnectHandler
type ErrorHandler = server.ErrorHandler
type AckFunc = server.AckFunc
type BinaryAckFunc = server.BinaryAckFunc

type Server struct{ inner *server.Server }
func New(opts *Options) *Server { return &Server{inner: server.New(opts)} }
func (s *Server) ServeHTTP(w http.ResponseWriter,r *http.Request){s.inner.ServeHTTP(w,r)}
func (s *Server) Serve() error{return s.inner.Serve()}
func (s *Server) Close() error{return s.inner.Close()}
func (s *Server) Count() int{return s.inner.Count()}
func (s *Server) OnConnect(ns string,fn ConnectHandler){s.inner.OnConnect(ns,fn)}
func (s *Server) OnDisconnect(ns string,fn DisconnectHandler){s.inner.OnDisconnect(ns,fn)}
func (s *Server) OnError(ns string,fn ErrorHandler){s.inner.OnError(ns,fn)}
func (s *Server) OnEvent(ns,event string,fn EventHandler){s.inner.OnEvent(ns,event,fn)}
func (s *Server) OnBinaryEvent(ns,event string,fn BinaryEventHandler){s.inner.OnBinaryEvent(ns,event,fn)}
func (s *Server) JoinRoom(ns,room string,c Conn){s.inner.JoinRoom(ns,room,c)}
func (s *Server) LeaveRoom(ns,room string,c Conn){s.inner.LeaveRoom(ns,room,c)}
func (s *Server) LeaveAllRooms(ns string,c Conn){s.inner.LeaveAllRooms(ns,c)}
func (s *Server) ClearRoom(ns,room string){s.inner.ClearRoom(ns,room)}
func (s *Server) RoomLen(ns,room string) int{return s.inner.RoomLen(ns,room)}
func (s *Server) Rooms(ns string) []string{return s.inner.Rooms(ns)}
func (s *Server) RoomMembers(ns,room string) []Conn{return s.inner.RoomMembers(ns,room)}
func (s *Server) ForEachInRoom(ns,room string,fn func(Conn)){s.inner.ForEachInRoom(ns,room,fn)}
func (s *Server) ToRoom(ns,room,event string,skip Conn,args ...interface{}){s.inner.ToRoom(ns,room,event,skip,args...)}
func (s *Server) ToNamespace(ns,event string,args ...interface{}){s.inner.ToNamespace(ns,event,args...)}
func Unmarshal(data json.RawMessage,v interface{}) error{return json.Unmarshal(data,v)}
