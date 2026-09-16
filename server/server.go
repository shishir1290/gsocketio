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

const eioMessage = byte('4')
const MaxRoomsPerConn = 100
func wrapEIO(p []byte) []byte { out:=make([]byte,len(p)+1);out[0]=eioMessage;copy(out[1:],p);return out }

type Conn interface { ID() string; Namespace() string; Emit(string,...interface{}) error; Join(string); Leave(string); Rooms() []string; Context() interface{}; SetContext(interface{}); Close() error }
type AckFunc func([]json.RawMessage,error)
type conn struct{id,ns string;tr transport.Conn;srv *Server;sendCh chan []byte;closed uint32;closeOnce,teardownOnce sync.Once;ctxMu sync.RWMutex;ctx interface{};roomMu sync.RWMutex;joined map[string]struct{};ackSeq atomic.Uint64;ackMu sync.Mutex;acks map[uint64]AckFunc}
func newConn(id,ns string,tr transport.Conn,srv *Server)*conn{return &conn{id:id,ns:ns,tr:tr,srv:srv,sendCh:make(chan []byte,512),joined:make(map[string]struct{}),acks:make(map[uint64]AckFunc)}}
func(c *conn)ID()string{return c.id};func(c *conn)Namespace()string{return c.ns};func(c *conn)Context()interface{}{c.ctxMu.RLock();defer c.ctxMu.RUnlock();return c.ctx};func(c *conn)SetContext(v interface{}){c.ctxMu.Lock();c.ctx=v;c.ctxMu.Unlock()}
func(c *conn)Rooms()[]string{c.roomMu.RLock();defer c.roomMu.RUnlock();out:=make([]string,0,len(c.joined));for r:=range c.joined{out=append(out,r)};return out}
func(c *conn)Join(room string){if room==""{return};c.roomMu.Lock();if _,ok:=c.joined[room];ok{c.roomMu.Unlock();return};if len(c.joined)>=MaxRoomsPerConn{c.roomMu.Unlock();return};c.joined[room]=struct{}{};c.roomMu.Unlock();if n:=c.srv.namespace(c.ns);n!=nil{n.rooms.Join(room,c)}}
func(c *conn)Leave(room string){c.roomMu.Lock();delete(c.joined,room);c.roomMu.Unlock();if n:=c.srv.namespace(c.ns);n!=nil{n.rooms.Leave(room,c)}}
func(c *conn)leaveAll(){if n:=c.srv.namespace(c.ns);n!=nil{n.rooms.LeaveAll(c)};c.roomMu.Lock();c.joined=make(map[string]struct{});c.roomMu.Unlock()}
func(c *conn)Emit(event string,args ...interface{})error{data,err:=packet.BuildEventData(event,args...);if err!=nil{return fmt.Errorf("conn.Emit: %w",err)};raw,err:=packet.Encode(&packet.Packet{Type:packet.TypeEvent,Namespace:c.ns,Data:data});if err!=nil{return err};return c.enqueue(raw)}
func(c *conn)EmitWithAck(event string,fn AckFunc,args ...interface{})error{data,err:=packet.BuildEventData(event,args...);if err!=nil{return err};seq:=c.ackSeq.Add(1);if seq>uint64(^uint(0)>>1){return errors.New("gsocketio: ack id overflow")};id:=int(seq);c.ackMu.Lock();c.acks[seq]=fn;c.ackMu.Unlock();raw,err:=packet.Encode(&packet.Packet{Type:packet.TypeEvent,Namespace:c.ns,ID:&id,Data:data});if err!=nil{c.ackMu.Lock();delete(c.acks,seq);c.ackMu.Unlock();return err};if err=c.enqueue(raw);err!=nil{c.ackMu.Lock();delete(c.acks,seq);c.ackMu.Unlock();return err};return nil}
func(c *conn)enqueue(raw []byte)error{if atomic.LoadUint32(&c.closed)==1{return errors.New("gsocketio: connection already closed")};p:=wrapEIO(raw);select{case c.sendCh<-p:return nil;case<-c.tr.Done():return errors.New("gsocketio: connection closed");default:return errors.New("gsocketio: send buffer full")}}
func(c *conn)fireAck(id uint64,args []json.RawMessage,err error){c.ackMu.Lock();fn,ok:=c.acks[id];if ok{delete(c.acks,id)};c.ackMu.Unlock();if ok&&fn!=nil{fn(args,err)}}
func(c *conn)Close()error{var err error;c.closeOnce.Do(func(){atomic.StoreUint32(&c.closed,1);c.leaveAll();err=c.tr.Close()});return err}

type EventHandler func(Conn,[]json.RawMessage)
type ConnectHandler func(Conn)error
type DisconnectHandler func(Conn,string)
type ErrorHandler func(Conn,error)
type namespace struct{name string;rooms *rooms.Manager;mu sync.RWMutex;onConnect ConnectHandler;onDisconnect DisconnectHandler;onError ErrorHandler;events map[string]EventHandler}
func newNamespace(n string)*namespace{return &namespace{name:n,rooms:rooms.New(),events:make(map[string]EventHandler)}}
func(n *namespace)setConnect(f ConnectHandler){n.mu.Lock();n.onConnect=f;n.mu.Unlock()}
func(n *namespace)setDisconnect(f DisconnectHandler){n.mu.Lock();n.onDisconnect=f;n.mu.Unlock()}
func(n *namespace)setError(f ErrorHandler){n.mu.Lock();n.onError=f;n.mu.Unlock()}
func(n *namespace)setEvent(e string,f EventHandler){n.mu.Lock();n.events[e]=f;n.mu.Unlock()}
func(n *namespace)getEvent(e string)EventHandler{n.mu.RLock();defer n.mu.RUnlock();return n.events[e]}

type Server struct{tr *transport.Server;nsMu sync.RWMutex;namespaces map[string]*namespace;connCount int64;connsMu sync.RWMutex;conns map[string]*conn}
func New(opts *transport.Options)*Server{return &Server{tr:transport.NewServer(opts),namespaces:make(map[string]*namespace),conns:make(map[string]*conn)}}
func(s *Server)ServeHTTP(w http.ResponseWriter,r *http.Request){s.tr.ServeHTTP(w,r)}
func(s *Server)Serve()error{for{tc,err:=s.tr.Accept();if err!=nil{return err};go s.handleConn(tc)}}
func(s *Server)Close()error{s.connsMu.RLock();cs:=make([]*conn,0,len(s.conns));for _,c:=range s.conns{cs=append(cs,c)};s.connsMu.RUnlock();for _,c:=range cs{n:=s.namespace(c.ns);if n!=nil{s.teardown(c,n,"server closed")}};return s.tr.Close()}
func(s *Server)Count()int{return int(atomic.LoadInt64(&s.connCount))}
func(s *Server)OnConnect(ns string,f ConnectHandler){s.ensureNamespace(ns).setConnect(f)}
func(s *Server)OnDisconnect(ns string,f DisconnectHandler){s.ensureNamespace(ns).setDisconnect(f)}
func(s *Server)OnError(ns string,f ErrorHandler){s.ensureNamespace(ns).setError(f)}
func(s *Server)OnEvent(ns,e string,f EventHandler){s.ensureNamespace(ns).setEvent(e,f)}
func(s *Server)JoinRoom(ns,room string,c Conn){if n:=s.namespace(ns);n!=nil{n.rooms.Join(room,c)}}
func(s *Server)LeaveRoom(ns,room string,c Conn){if n:=s.namespace(ns);n!=nil{n.rooms.Leave(room,c)}}
func(s *Server)LeaveAllRooms(ns string,c Conn){if n:=s.namespace(ns);n!=nil{n.rooms.LeaveAll(c)}}
func(s *Server)ClearRoom(ns,room string){if n:=s.namespace(ns);n!=nil{n.rooms.Clear(room)}}
func(s *Server)RoomLen(ns,room string)int{if n:=s.namespace(ns);n!=nil{return n.rooms.Len(room)};return 0}
func(s *Server)Rooms(ns string)[]string{if n:=s.namespace(ns);n!=nil{return n.rooms.Names()};return nil}
func(s *Server)RoomMembers(ns,room string)[]Conn{n:=s.namespace(ns);if n==nil{return nil};ms:=n.rooms.Members(room);out:=make([]Conn,0,len(ms));for _,m:=range ms{if c,ok:=m.(Conn);ok{out=append(out,c)}};return out}
func(s *Server)ForEachInRoom(ns,room string,f func(Conn)){if n:=s.namespace(ns);n!=nil{n.rooms.ForEach(room,func(m rooms.Member){if c,ok:=m.(Conn);ok{f(c)}})}}
func(s *Server)ToRoom(ns,room,event string,skip Conn,args ...interface{}){n:=s.namespace(ns);if n==nil{return};ids:=map[string]struct{}{};if skip!=nil{ids[skip.ID()]=struct{}{}};n.rooms.Send(room,event,ids,args...)}
func(s *Server)ToNamespace(ns,event string,args ...interface{}){if n:=s.namespace(ns);n!=nil{n.rooms.SendAll(event,args...)}}
func(s *Server)ensureNamespace(ns string)*namespace{ns=packet.NormalizeNS(ns);s.nsMu.Lock();defer s.nsMu.Unlock();if n:=s.namespaces[ns];n!=nil{return n};n:=newNamespace(ns);s.namespaces[ns]=n;return n}
func(s *Server)namespace(ns string)*namespace{ns=packet.NormalizeNS(ns);s.nsMu.RLock();defer s.nsMu.RUnlock();return s.namespaces[ns]}
func(s *Server)handleConn(tc transport.Conn){_,raw,err:=tc.ReadMessage();if err!=nil{_=tc.Close();return};raw=stripEIO(raw);pkt,err:=packet.Decode(raw);if err!=nil{_=tc.Close();return};if pkt.Type!=packet.TypeConnect{_=tc.Close();return};ns:=pkt.Namespace;n:=s.namespace(ns);if n==nil{if ns!="/"{s.sendConnectError(tc,ns,"namespace not found");_=tc.Close();return};n=s.ensureNamespace(ns)};c:=newConn(transport.NewSID(),ns,tc,s);s.connsMu.Lock();s.conns[c.id]=c;s.connsMu.Unlock();atomic.AddInt64(&s.connCount,1);ack:=&packet.Packet{Type:packet.TypeConnect,Namespace:ns,Data:mustMarshal(map[string]string{"sid":c.id})};if b,e:=packet.Encode(ack);e==nil{if e=tc.WriteText(wrapEIO(b));e!=nil{s.teardown(c,n,"connect write failed");return}};n.mu.RLock();fn:=n.onConnect;n.mu.RUnlock();if fn!=nil{if e:=fn(c);e!=nil{s.sendConnectError(tc,ns,"rejected");s.teardown(c,n,"rejected by OnConnect");return}};go s.writePump(c);s.readPump(c,n)}
func(s *Server)sendConnectError(tc transport.Conn,ns,msg string){p:=&packet.Packet{Type:packet.TypeConnectError,Namespace:ns,Data:mustMarshal(map[string]string{"message":msg})};if b,e:=packet.Encode(p);e==nil{_=tc.WriteText(wrapEIO(b))}}
func(s *Server)readPump(c *conn,n *namespace){defer s.teardown(c,n,"transport closed");for{_,raw,err:=c.tr.ReadMessage();if err!=nil{return};raw=stripEIO(raw);p,err:=packet.Decode(raw);if err!=nil{n.mu.RLock();f:=n.onError;n.mu.RUnlock();if f!=nil{f(c,err)};continue};switch p.Type{case packet.TypeDisconnect:return;case packet.TypeEvent:s.dispatchEvent(c,n,p);case packet.TypeAck:s.dispatchAck(c,p)}}}
func(s *Server)writePump(c *conn){for{select{case p:=<-c.sendCh:if err:=c.tr.WriteText(p);err!=nil{return};case<-c.tr.Done():return}}}
func(s *Server)dispatchEvent(c *conn,n *namespace,p *packet.Packet){name,err:=packet.EventName(p.Data);if err!=nil{n.mu.RLock();f:=n.onError;n.mu.RUnlock();if f!=nil{f(c,err)};return};args,err:=packet.EventArgs(p.Data);if err!=nil{n.mu.RLock();f:=n.onError;n.mu.RUnlock();if f!=nil{f(c,err)};return};fn:=n.getEvent(name);if p.ID!=nil{ackID:=*p.ID;go func(){if fn!=nil{fn(c,args)};data,_:=packet.BuildAckData();raw,e:=packet.Encode(&packet.Packet{Type:packet.TypeAck,Namespace:c.ns,ID:&ackID,Data:data});if e==nil{_=c.enqueue(raw)}}();return};if fn!=nil{go fn(c,args)}}
func(s *Server)dispatchAck(c *conn,p *packet.Packet){if p.ID==nil{return};args,err:=packet.EventArgs(p.Data);if err!=nil{c.fireAck(uint64(*p.ID),nil,err);return};c.fireAck(uint64(*p.ID),args,nil)}
func(s *Server)teardown(c *conn,n *namespace,reason string){c.teardownOnce.Do(func(){_=c.Close();s.connsMu.Lock();delete(s.conns,c.id);s.connsMu.Unlock();s.tr.RemoveConn(c.tr);atomic.AddInt64(&s.connCount,-1);n.mu.RLock();f:=n.onDisconnect;n.mu.RUnlock();if f!=nil{f(c,reason)}})}
func stripEIO(raw []byte)[]byte{if len(raw)>0&&raw[0]==eioMessage{return raw[1:]};return raw}
func mustMarshal(v interface{})json.RawMessage{b,e:=json.Marshal(v);if e!=nil{logger.Error("marshal error: %v",e);return json.RawMessage(`{}`)};return b}
