package packet

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

type Type byte

const (
	TypeConnect Type = '0'
	TypeDisconnect Type = '1'
	TypeEvent Type = '2'
	TypeAck Type = '3'
	TypeConnectError Type = '4'
)

func (t Type) String() string { switch t { case TypeConnect:return "CONNECT"; case TypeDisconnect:return "DISCONNECT"; case TypeEvent:return "EVENT"; case TypeAck:return "ACK"; case TypeConnectError:return "CONNECT_ERROR"; default:return fmt.Sprintf("UNKNOWN(%c)",t) } }
func (t Type) Valid() bool { return t>=TypeConnect && t<=TypeConnectError }

type Packet struct { Type Type; Namespace string; ID *int; Data json.RawMessage }
type ErrInvalidPacket struct { Reason string; Raw string }
func (e *ErrInvalidPacket) Error() string { return fmt.Sprintf("packet: invalid packet %q: %s",e.Raw,e.Reason) }
func invalid(raw,reason string) error { return &ErrInvalidPacket{Reason:reason,Raw:raw} }

func Encode(p *Packet)([]byte,error){
	if p==nil||!p.Type.Valid(){return nil,fmt.Errorf("packet: unknown type")}
	ns:=p.Namespace;if ns==""{ns="/"};var b bytes.Buffer;b.WriteByte(byte(p.Type));if ns!="/"{if !strings.HasPrefix(ns,"/"){return nil,fmt.Errorf("packet: invalid namespace")};b.WriteString(ns);b.WriteByte(',')};if p.ID!=nil{if *p.ID<0{return nil,fmt.Errorf("packet: negative ack id")};b.WriteString(strconv.Itoa(*p.ID))};if len(p.Data)>0{b.Write(p.Data)};return b.Bytes(),nil
}
func Decode(raw []byte)(*Packet,error){
	s:=string(raw);if s==""{return nil,invalid(s,"empty message")};p:=&Packet{Namespace:"/",Type:Type(s[0])};if !p.Type.Valid(){return nil,invalid(s,"unknown type")};rest:=s[1:]
	if strings.HasPrefix(rest,"/"){idx:=strings.IndexByte(rest,',');if idx<0{return nil,invalid(s,"namespace is missing comma")};p.Namespace=rest[:idx];rest=rest[idx+1:]}
	i:=0;for i<len(rest)&&rest[i]>='0'&&rest[i]<='9'{i++};if i>0{id64,err:=strconv.ParseInt(rest[:i],10,0);if err!=nil||id64<0{return nil,invalid(s,"ack id out of range")};id:=int(id64);p.ID=&id;rest=rest[i:]}
	if len(rest)>0{p.Data=json.RawMessage(rest);if !json.Valid(p.Data){return nil,invalid(s,"invalid JSON data")}}
	return p,nil
}
func EventName(data json.RawMessage)(string,error){if len(data)==0{return "",errors.New("packet: empty data")};var a []json.RawMessage;if err:=json.Unmarshal(data,&a);err!=nil{return "",fmt.Errorf("packet: data is not a JSON array: %w",err)};if len(a)==0{return "",errors.New("packet: empty event array")};var name string;if err:=json.Unmarshal(a[0],&name);err!=nil{return "",fmt.Errorf("packet: event name is not a string: %w",err)};if name==""{return "",errors.New("packet: empty event name")};return name,nil}
func EventArgs(data json.RawMessage)([]json.RawMessage,error){var a []json.RawMessage;if err:=json.Unmarshal(data,&a);err!=nil{return nil,fmt.Errorf("packet: data is not a JSON array: %w",err)};if len(a)<=1{return nil,nil};return a[1:],nil}
func BuildEventData(event string,args ...interface{})(json.RawMessage,error){p:=make([]interface{},0,len(args)+1);p=append(p,event);p=append(p,args...);b,err:=json.Marshal(p);if err!=nil{return nil,fmt.Errorf("packet: marshal event data: %w",err)};return json.RawMessage(b),nil}
func BuildAckData(values ...interface{})(json.RawMessage,error){if len(values)==0{return json.RawMessage("[]"),nil};b,err:=json.Marshal(values);if err!=nil{return nil,fmt.Errorf("packet: marshal ack data: %w",err)};return json.RawMessage(b),nil}
func NormalizeNS(ns string)string{ns=strings.TrimSpace(ns);if ns==""{return "/"};if !strings.HasPrefix(ns,"/"){return "/"+ns};return ns}
