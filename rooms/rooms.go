// Package rooms provides thread-safe room management and broadcast primitives.
// No external dependencies — pure Go standard library.
package rooms

import (
	"sync"
)

// ─────────────────────────────────────────────────────────────────────────────
// Member interface
// ─────────────────────────────────────────────────────────────────────────────

// Member is anything that can receive an emitted event and has a unique ID.
// The socketio.Conn type satisfies this interface.
type Member interface {
	ID() string
	Emit(event string, args ...interface{}) error
}

// ─────────────────────────────────────────────────────────────────────────────
// room — internal per-room state
// ─────────────────────────────────────────────────────────────────────────────

type room struct {
	mu      sync.RWMutex
	members map[string]Member
}

func newRoom() *room {
	return &room{members: make(map[string]Member)}
}

// add inserts m; returns false if already present.
func (r *room) add(m Member) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.members[m.ID()]; ok {
		return false
	}
	r.members[m.ID()] = m
	return true
}

// remove deletes m; returns false if not present.
func (r *room) remove(m Member) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.members[m.ID()]; !ok {
		return false
	}
	delete(r.members, m.ID())
	return true
}

// has reports whether m is in the room.
func (r *room) has(id string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.members[id]
	return ok
}

// len returns the member count.
func (r *room) len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.members)
}

// snapshot returns a stable slice of all current members.
func (r *room) snapshot() []Member {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Member, 0, len(r.members))
	for _, m := range r.members {
		out = append(out, m)
	}
	return out
}

// ─────────────────────────────────────────────────────────────────────────────
// Manager — collection of named rooms
// ─────────────────────────────────────────────────────────────────────────────

// Manager manages a set of named rooms and provides broadcast operations.
// All methods are safe for concurrent use.
type Manager struct {
	mu    sync.RWMutex
	rooms map[string]*room
}

// New returns an empty Manager.
func New() *Manager {
	return &Manager{rooms: make(map[string]*room)}
}

// ── membership ────────────────────────────────────────────────────────────────

// Join adds m to roomName, creating the room if needed.
// Returns false if m was already in the room.
func (mg *Manager) Join(roomName string, m Member) bool {
	mg.mu.Lock()
	r, ok := mg.rooms[roomName]
	if !ok {
		r = newRoom()
		mg.rooms[roomName] = r
	}
	mg.mu.Unlock()
	return r.add(m)
}

// Leave removes m from roomName.
// Returns false if the room or m was not found.
func (mg *Manager) Leave(roomName string, m Member) bool {
	mg.mu.RLock()
	r, ok := mg.rooms[roomName]
	mg.mu.RUnlock()
	if !ok {
		return false
	}
	return r.remove(m)
}

// LeaveAll removes m from every room it belongs to.
func (mg *Manager) LeaveAll(m Member) {
	mg.mu.RLock()
	rs := make([]*room, 0, len(mg.rooms))
	for _, r := range mg.rooms {
		rs = append(rs, r)
	}
	mg.mu.RUnlock()
	for _, r := range rs {
		r.remove(m)
	}
}

// InRoom reports whether m is currently in roomName.
func (mg *Manager) InRoom(roomName string, m Member) bool {
	mg.mu.RLock()
	r, ok := mg.rooms[roomName]
	mg.mu.RUnlock()
	if !ok {
		return false
	}
	return r.has(m.ID())
}

// ── query ─────────────────────────────────────────────────────────────────────

// Len returns the number of members in roomName; 0 for unknown rooms.
func (mg *Manager) Len(roomName string) int {
	mg.mu.RLock()
	r, ok := mg.rooms[roomName]
	mg.mu.RUnlock()
	if !ok {
		return 0
	}
	return r.len()
}

// Names returns the list of all room names.
func (mg *Manager) Names() []string {
	mg.mu.RLock()
	defer mg.mu.RUnlock()
	names := make([]string, 0, len(mg.rooms))
	for n := range mg.rooms {
		names = append(names, n)
	}
	return names
}

// Members returns a snapshot of all members in roomName.
func (mg *Manager) Members(roomName string) []Member {
	mg.mu.RLock()
	r, ok := mg.rooms[roomName]
	mg.mu.RUnlock()
	if !ok {
		return nil
	}
	return r.snapshot()
}

// ForEach calls fn for each member of roomName.
// fn is called outside the room lock, so it is safe to call Emit, Join, etc.
func (mg *Manager) ForEach(roomName string, fn func(Member)) {
	for _, m := range mg.Members(roomName) {
		fn(m)
	}
}

// ── clear ─────────────────────────────────────────────────────────────────────

// Clear removes the named room entirely.
func (mg *Manager) Clear(roomName string) {
	mg.mu.Lock()
	delete(mg.rooms, roomName)
	mg.mu.Unlock()
}

// ── broadcast ─────────────────────────────────────────────────────────────────

// Send emits event+args to all members of roomName.
// Members whose ID is in skipIDs are skipped.
// Errors from individual Emit calls are ignored (best-effort delivery).
func (mg *Manager) Send(roomName, event string, skipIDs map[string]struct{}, args ...interface{}) {
	for _, m := range mg.Members(roomName) {
		if _, skip := skipIDs[m.ID()]; skip {
			continue
		}
		_ = m.Emit(event, args...)
	}
}

// SendAll emits event+args to every member across all rooms.
// Each member receives at most one call even if in multiple rooms.
func (mg *Manager) SendAll(event string, args ...interface{}) {
	seen := make(map[string]struct{})

	mg.mu.RLock()
	rs := make([]*room, 0, len(mg.rooms))
	for _, r := range mg.rooms {
		rs = append(rs, r)
	}
	mg.mu.RUnlock()

	for _, r := range rs {
		for _, m := range r.snapshot() {
			if _, ok := seen[m.ID()]; ok {
				continue
			}
			seen[m.ID()] = struct{}{}
			_ = m.Emit(event, args...)
		}
	}
}
