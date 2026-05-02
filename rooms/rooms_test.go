package rooms_test

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/shishir1290/gsocketio/rooms"
)

// ─────────────────────────────────────────────────────────────────────────────
// mock member
// ─────────────────────────────────────────────────────────────────────────────

type mockMember struct {
	id      string
	emitted int32 // atomic count of Emit calls
}

func (m *mockMember) ID() string { return m.id }
func (m *mockMember) Emit(event string, args ...interface{}) error {
	atomic.AddInt32(&m.emitted, 1)
	return nil
}
func (m *mockMember) count() int { return int(atomic.LoadInt32(&m.emitted)) }

func newMember(id string) *mockMember { return &mockMember{id: id} }

// ─────────────────────────────────────────────────────────────────────────────
// Join / Leave
// ─────────────────────────────────────────────────────────────────────────────

func TestJoin(t *testing.T) {
	mg := rooms.New()
	m := newMember("a")
	if !mg.Join("room1", m) {
		t.Error("first Join should return true")
	}
	if mg.Len("room1") != 1 {
		t.Errorf("Len after Join: want 1 got %d", mg.Len("room1"))
	}
}

func TestJoinIdempotent(t *testing.T) {
	mg := rooms.New()
	m := newMember("a")
	mg.Join("r", m)
	if mg.Join("r", m) {
		t.Error("duplicate Join should return false")
	}
	if mg.Len("r") != 1 {
		t.Errorf("Len after duplicate Join: want 1 got %d", mg.Len("r"))
	}
}

func TestLeave(t *testing.T) {
	mg := rooms.New()
	m := newMember("a")
	mg.Join("r", m)
	if !mg.Leave("r", m) {
		t.Error("Leave of existing member should return true")
	}
	if mg.Len("r") != 0 {
		t.Errorf("Len after Leave: want 0 got %d", mg.Len("r"))
	}
}

func TestLeaveNotPresent(t *testing.T) {
	mg := rooms.New()
	m := newMember("a")
	if mg.Leave("ghost", m) {
		t.Error("Leave of non-existent room should return false")
	}
}

func TestLeaveWrongMember(t *testing.T) {
	mg := rooms.New()
	a := newMember("a")
	b := newMember("b")
	mg.Join("r", a)
	if mg.Leave("r", b) {
		t.Error("Leave of member not in room should return false")
	}
	if mg.Len("r") != 1 {
		t.Errorf("Len unchanged after bad Leave: want 1 got %d", mg.Len("r"))
	}
}

func TestLeaveAll(t *testing.T) {
	mg := rooms.New()
	m := newMember("x")
	mg.Join("r1", m)
	mg.Join("r2", m)
	mg.Join("r3", m)
	mg.LeaveAll(m)
	for _, r := range []string{"r1", "r2", "r3"} {
		if mg.Len(r) != 0 {
			t.Errorf("after LeaveAll, room %s: want 0 got %d", r, mg.Len(r))
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// InRoom
// ─────────────────────────────────────────────────────────────────────────────

func TestInRoom(t *testing.T) {
	mg := rooms.New()
	m := newMember("a")
	mg.Join("r", m)
	if !mg.InRoom("r", m) {
		t.Error("InRoom should be true after Join")
	}
	mg.Leave("r", m)
	if mg.InRoom("r", m) {
		t.Error("InRoom should be false after Leave")
	}
}

func TestInRoomNonexistent(t *testing.T) {
	mg := rooms.New()
	m := newMember("a")
	if mg.InRoom("ghost", m) {
		t.Error("InRoom on nonexistent room should be false")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Len
// ─────────────────────────────────────────────────────────────────────────────

func TestLenNonexistent(t *testing.T) {
	mg := rooms.New()
	if mg.Len("ghost") != 0 {
		t.Error("Len of nonexistent room should be 0")
	}
}

func TestLenMultiple(t *testing.T) {
	mg := rooms.New()
	for i := 0; i < 10; i++ {
		mg.Join("r", newMember(fmt.Sprintf("m%d", i)))
	}
	if mg.Len("r") != 10 {
		t.Errorf("Len: want 10 got %d", mg.Len("r"))
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Names
// ─────────────────────────────────────────────────────────────────────────────

func TestNames(t *testing.T) {
	mg := rooms.New()
	for _, r := range []string{"alpha", "beta", "gamma"} {
		mg.Join(r, newMember("x"))
	}
	names := mg.Names()
	sort.Strings(names)
	want := []string{"alpha", "beta", "gamma"}
	for i, n := range want {
		if names[i] != n {
			t.Errorf("names[%d]: want %q got %q", i, n, names[i])
		}
	}
}

func TestNamesEmpty(t *testing.T) {
	mg := rooms.New()
	if names := mg.Names(); len(names) != 0 {
		t.Errorf("empty manager should have 0 names, got %d", len(names))
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Members / ForEach
// ─────────────────────────────────────────────────────────────────────────────

func TestMembers(t *testing.T) {
	mg := rooms.New()
	a, b, c := newMember("a"), newMember("b"), newMember("c")
	mg.Join("r", a)
	mg.Join("r", b)
	mg.Join("r", c)
	members := mg.Members("r")
	if len(members) != 3 {
		t.Errorf("want 3 members got %d", len(members))
	}
}

func TestMembersNonexistent(t *testing.T) {
	mg := rooms.New()
	if m := mg.Members("ghost"); m != nil {
		t.Errorf("nonexistent room should return nil, got %v", m)
	}
}

func TestForEach(t *testing.T) {
	mg := rooms.New()
	for i := 0; i < 5; i++ {
		mg.Join("r", newMember(fmt.Sprintf("m%d", i)))
	}
	var count int32
	mg.ForEach("r", func(m rooms.Member) { atomic.AddInt32(&count, 1) })
	if count != 5 {
		t.Errorf("ForEach count: want 5 got %d", count)
	}
}

func TestForEachEmpty(t *testing.T) {
	mg := rooms.New()
	called := false
	mg.ForEach("ghost", func(rooms.Member) { called = true })
	if called {
		t.Error("ForEach should not be called for nonexistent room")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Clear
// ─────────────────────────────────────────────────────────────────────────────

func TestClear(t *testing.T) {
	mg := rooms.New()
	for i := 0; i < 5; i++ {
		mg.Join("r", newMember(fmt.Sprintf("m%d", i)))
	}
	mg.Clear("r")
	if mg.Len("r") != 0 {
		t.Errorf("after Clear Len: want 0 got %d", mg.Len("r"))
	}
}

func TestClearNonexistent(t *testing.T) {
	mg := rooms.New()
	mg.Clear("ghost") // should not panic
}

// ─────────────────────────────────────────────────────────────────────────────
// Send
// ─────────────────────────────────────────────────────────────────────────────

func TestSend(t *testing.T) {
	mg := rooms.New()
	a, b, c := newMember("a"), newMember("b"), newMember("c")
	mg.Join("r", a)
	mg.Join("r", b)
	mg.Join("r", c)
	mg.Send("r", "ping", nil)
	for _, m := range []*mockMember{a, b, c} {
		if m.count() != 1 {
			t.Errorf("%s: want 1 emit got %d", m.id, m.count())
		}
	}
}

func TestSendWithSkip(t *testing.T) {
	mg := rooms.New()
	sender := newMember("sender")
	other := newMember("other")
	mg.Join("r", sender)
	mg.Join("r", other)

	skip := map[string]struct{}{"sender": {}}
	mg.Send("r", "msg", skip)

	if sender.count() != 0 {
		t.Error("sender should be skipped")
	}
	if other.count() != 1 {
		t.Error("other should receive 1 emit")
	}
}

func TestSendNonexistentRoom(t *testing.T) {
	mg := rooms.New()
	mg.Send("ghost", "event", nil) // must not panic
}

func TestSendEmptySkipMap(t *testing.T) {
	mg := rooms.New()
	m := newMember("x")
	mg.Join("r", m)
	mg.Send("r", "ping", map[string]struct{}{})
	if m.count() != 1 {
		t.Errorf("want 1 emit got %d", m.count())
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// SendAll
// ─────────────────────────────────────────────────────────────────────────────

func TestSendAll(t *testing.T) {
	mg := rooms.New()
	a := newMember("a")
	b := newMember("b")
	// a is in two rooms — should receive only once
	mg.Join("r1", a)
	mg.Join("r2", a)
	mg.Join("r2", b)
	mg.SendAll("ping")
	if a.count() != 1 {
		t.Errorf("a: want 1 got %d", a.count())
	}
	if b.count() != 1 {
		t.Errorf("b: want 1 got %d", b.count())
	}
}

func TestSendAllEmpty(t *testing.T) {
	mg := rooms.New()
	mg.SendAll("ping") // must not panic
}

// ─────────────────────────────────────────────────────────────────────────────
// Concurrent safety
// ─────────────────────────────────────────────────────────────────────────────

func TestConcurrentJoinLeave(t *testing.T) {
	mg := rooms.New()
	var wg sync.WaitGroup
	for i := 0; i < 500; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			id := fmt.Sprintf("m%d", n)
			m := newMember(id)
			mg.Join("concurrent", m)
			mg.Leave("concurrent", m)
		}(i)
	}
	wg.Wait()
}

func TestConcurrentSend(t *testing.T) {
	mg := rooms.New()
	members := make([]*mockMember, 20)
	for i := range members {
		members[i] = newMember(fmt.Sprintf("m%d", i))
		mg.Join("r", members[i])
	}

	var wg sync.WaitGroup
	const broadcasts = 10
	for i := 0; i < broadcasts; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			mg.Send("r", "event", nil)
		}()
	}
	wg.Wait()

	for _, m := range members {
		if m.count() != broadcasts {
			t.Errorf("%s: want %d emits got %d", m.id, broadcasts, m.count())
		}
	}
}

func TestConcurrentLeaveAll(t *testing.T) {
	mg := rooms.New()
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			m := newMember(fmt.Sprintf("m%d", n))
			for _, r := range []string{"r1", "r2", "r3"} {
				mg.Join(r, m)
			}
			mg.LeaveAll(m)
		}(i)
	}
	wg.Wait()
}

func TestConcurrentSendAll(t *testing.T) {
	mg := rooms.New()
	m := newMember("singleton")
	for _, r := range []string{"r1", "r2", "r3", "r4", "r5"} {
		mg.Join(r, m)
	}
	var wg sync.WaitGroup
	const iters = 5
	for i := 0; i < iters; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			mg.SendAll("ping")
		}()
	}
	wg.Wait()
	// m is in 5 rooms but SendAll deduplicates → 1 emit per call × 5 calls
	if m.count() != iters {
		t.Errorf("SendAll dedup: want %d got %d", iters, m.count())
	}
}
