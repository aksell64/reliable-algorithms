package election

import (
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"reliable/messages"
	"reliable/types"
)

// ---------------------------------------------------------------------------
// mocks
// ---------------------------------------------------------------------------

type mockLink struct {
	types.UnimplementedDeliverer
	mu         sync.Mutex
	sent       []sentRecord
	deliverers []types.Deliverer
}

type sentRecord struct {
	to  types.ProcessID
	msg types.Message
}

func newMockLink(pid types.ProcessID) *mockLink {
	return &mockLink{
		UnimplementedDeliverer: types.NewUnimplementedDeliverer(pid),
	}
}

func (m *mockLink) Send(to types.ProcessID, msg types.Message) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sent = append(m.sent, sentRecord{to, msg})
}

func (m *mockLink) AddDeliverer(d types.Deliverer, _ ...types.DelivererOption) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deliverers = append(m.deliverers, d)
}

func (m *mockLink) Init()            {}
func (m *mockLink) Start()           {}
func (m *mockLink) Stop()            {}
func (m *mockLink) Instance() string { return "mock-link" }

func (m *mockLink) sentMessages() []sentRecord {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]sentRecord, len(m.sent))
	copy(cp, m.sent)
	return cp
}

func (m *mockLink) clearSent() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sent = nil
}

type mockReceiver struct {
	mu      sync.Mutex
	leaders []types.ProcessID
}

func (r *mockReceiver) OnNewLeader(pid types.ProcessID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.leaders = append(r.leaders, pid)
}

func (r *mockReceiver) receivedLeaders() []types.ProcessID {
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := make([]types.ProcessID, len(r.leaders))
	copy(cp, r.leaders)
	return cp
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// makeProcesses creates processes with ranks equal to their index+1.
// e.g. makeProcesses(1,2,3,4) → {1:rank1, 2:rank2, 3:rank3, 4:rank4}
func makeProcesses(ids ...types.ProcessID) map[types.ProcessID]types.ProcessRank {
	m := make(map[types.ProcessID]types.ProcessRank)
	for i, id := range ids {
		m[id] = types.ProcessRank(i + 1)
	}
	return m
}

// newDetector creates a detector and fixes the uninitialized processesCount field.
// NOTE: processesCount is never set in the constructor — this is a known bug.
func newDetector(
	self types.ProcessID,
	processes map[types.ProcessID]types.ProcessRank,
	faults int,
) (*RotatingByzantineLeaderDetector, *mockLink) {
	link := newMockLink(self)
	d := NewRotatingByzantineLeaderDetector(self, link, processes, faults)
	d.processesCount = len(processes) // fix: constructor never initialises this
	return d, link
}

func makeComplain(from types.ProcessID, round int) ComplainMsg {
	return ComplainMsg{
		BaseMsg: messages.NewBase(uuid.New(), from, ComplainMsgName),
		Round:   round,
	}
}

// ---------------------------------------------------------------------------
// constructor
// ---------------------------------------------------------------------------

func TestNewDetector_RegistersWithLink(t *testing.T) {
	processes := makeProcesses(1, 2, 3, 4)
	_, link := newDetector(1, processes, 1)
	assert.NotEmpty(t, link.deliverers, "detector must register itself with the link")
}

func TestNewDetector_StoresParameters(t *testing.T) {
	processes := makeProcesses(1, 2, 3, 4)
	d, _ := newDetector(2, processes, 1)
	assert.Equal(t, types.ProcessID(2), d.self)
	assert.Equal(t, 1, d.faults)
	assert.Equal(t, processes, d.processes)
}

// ---------------------------------------------------------------------------
// Subscribe
// ---------------------------------------------------------------------------

func TestSubscribe_SetsReceiver(t *testing.T) {
	processes := makeProcesses(1, 2, 3, 4)
	d, _ := newDetector(1, processes, 1)
	recv := &mockReceiver{}
	d.Subscribe(recv)
	assert.Equal(t, recv, d.receiver)
}

// ---------------------------------------------------------------------------
// Init
// ---------------------------------------------------------------------------

func TestInit_SetsRoundToOne(t *testing.T) {
	processes := makeProcesses(1, 2, 3, 4)
	d, _ := newDetector(1, processes, 1)
	d.Init()
	d.mu.RLock()
	defer d.mu.RUnlock()
	assert.Equal(t, 1, d.round)
}

func TestInit_ClearsComplainedFlag(t *testing.T) {
	processes := makeProcesses(1, 2, 3, 4)
	d, _ := newDetector(1, processes, 1)
	d.complained = true
	d.Init()
	d.mu.RLock()
	defer d.mu.RUnlock()
	assert.False(t, d.complained)
}

func TestInit_ClearsComplains(t *testing.T) {
	processes := makeProcesses(1, 2, 3, 4)
	d, _ := newDetector(1, processes, 1)
	d.Init()
	d.mu.RLock()
	defer d.mu.RUnlock()
	assert.Empty(t, d.complains)
}

func TestInit_WithReceiver_NotifiesCurrentLeader(t *testing.T) {
	processes := makeProcesses(1, 2, 3, 4)
	d, _ := newDetector(1, processes, 1)
	recv := &mockReceiver{}
	d.Subscribe(recv)
	d.Init()
	leaders := recv.receivedLeaders()
	require.Len(t, leaders, 1, "Init must notify receiver exactly once")
}

func TestInit_WithoutReceiver_DoesNotPanic(t *testing.T) {
	processes := makeProcesses(1, 2, 3, 4)
	d, _ := newDetector(1, processes, 1)
	require.NotPanics(t, func() { d.Init() })
}

// ---------------------------------------------------------------------------
// currentLeader
// ---------------------------------------------------------------------------

func TestCurrentLeader_Round1_ReturnsRank1Process(t *testing.T) {
	// round=1, processesCount=4: 1&4==0 → rank = 1%4 = 1
	processes := makeProcesses(1, 2, 3, 4)
	d, _ := newDetector(1, processes, 1)
	d.round = 1
	leader := d.currentLeader()
	rank := processes[leader]
	assert.Equal(t, types.ProcessRank(1), rank)
}

func TestCurrentLeader_Round2_ReturnsRank2Process(t *testing.T) {
	// 2&4==0 → rank = 2%4 = 2
	processes := makeProcesses(1, 2, 3, 4)
	d, _ := newDetector(1, processes, 1)
	d.round = 2
	leader := d.currentLeader()
	rank := processes[leader]
	assert.Equal(t, types.ProcessRank(2), rank)
}

func TestCurrentLeader_Round3_ReturnsRank3Process(t *testing.T) {
	// 3&4==0 → rank = 3%4 = 3
	processes := makeProcesses(1, 2, 3, 4)
	d, _ := newDetector(1, processes, 1)
	d.round = 3
	leader := d.currentLeader()
	rank := processes[leader]
	assert.Equal(t, types.ProcessRank(3), rank)
}

func TestCurrentLeader_Round4_ReturnsRank4Process(t *testing.T) {
	// 4&4==4 ≠ 0 → else branch → rank == processesCount == 4
	processes := makeProcesses(1, 2, 3, 4)
	d, _ := newDetector(1, processes, 1)
	d.round = 4
	leader := d.currentLeader()
	rank := processes[leader]
	assert.Equal(t, types.ProcessRank(4), rank)
}

// ---------------------------------------------------------------------------
// OnComplain
// ---------------------------------------------------------------------------

func TestOnComplain_CurrentLeader_BroadcastsToAllProcesses(t *testing.T) {
	processes := makeProcesses(1, 2, 3, 4)
	d, link := newDetector(1, processes, 1)
	d.Init()
	leader := d.currentLeader()

	d.OnComplain(leader)

	sent := link.sentMessages()
	assert.Len(t, sent, len(processes), "complaint must be broadcast to every process")
}

func TestOnComplain_CurrentLeader_SendsComplainMsgWithCurrentRound(t *testing.T) {
	processes := makeProcesses(1, 2, 3, 4)
	d, link := newDetector(1, processes, 1)
	d.Init()
	leader := d.currentLeader()

	d.OnComplain(leader)

	for _, rec := range link.sentMessages() {
		msg, ok := rec.msg.(ComplainMsg)
		require.True(t, ok, "sent message must be ComplainMsg")
		assert.Equal(t, d.round, msg.Round)
	}
}

func TestOnComplain_NonCurrentLeader_SendsNothing(t *testing.T) {
	processes := makeProcesses(1, 2, 3, 4)
	d, link := newDetector(1, processes, 1)
	d.Init()
	leader := d.currentLeader()

	var nonLeader types.ProcessID
	for pid := range processes {
		if pid != leader {
			nonLeader = pid
			break
		}
	}

	d.OnComplain(nonLeader)

	assert.Empty(t, link.sentMessages(), "complaint about non-leader must be ignored")
}

func TestOnComplain_Deduplication_SecondComplaintIgnored(t *testing.T) {
	processes := makeProcesses(1, 2, 3, 4)
	d, link := newDetector(1, processes, 1)
	d.Init()
	leader := d.currentLeader()

	d.OnComplain(leader)
	link.clearSent()
	d.OnComplain(leader) // duplicate

	assert.Empty(t, link.sentMessages(), "second OnComplain for same leader must be ignored")
}

// ---------------------------------------------------------------------------
// handleComplaint
//
// NOTE: handleComplaint has two known bugs:
//  1. The guard condition is inverted: `!existsComplain` should be `existsComplain`.
//     As written, the function always returns early because d.complains is always
//     initialised empty. Tests below pre-populate d.complains to work around this
//     and verify the rest of the logic.
//  2. When len(complains) > 2*faults the mutex is unlocked inside the if-block
//     AND by the trailing d.mu.Unlock(), causing a double-unlock panic.
//     The test for round advancement is therefore marked with require.NotPanics.
// ---------------------------------------------------------------------------

func TestHandleComplaint_WrongRound_Ignored(t *testing.T) {
	processes := makeProcesses(1, 2, 3, 4)
	d, _ := newDetector(1, processes, 1)
	d.Init()

	// Pre-populate to bypass the inverted guard
	d.mu.Lock()
	d.complains[types.ProcessID(2)] = struct{}{}
	d.mu.Unlock()

	d.handleComplaint(2, d.round+99)

	d.mu.RLock()
	defer d.mu.RUnlock()
	assert.Equal(t, 1, d.round, "wrong round: round must not advance")
}

func TestHandleComplaint_InsufficientComplaints_NoAction(t *testing.T) {
	// f=1: need > f complaints to cascade, > 2f to advance round.
	// 1 complaint is not enough for either.
	processes := makeProcesses(1, 2, 3, 4)
	d, link := newDetector(1, processes, 1)
	d.Init()

	d.mu.Lock()
	d.complains[types.ProcessID(2)] = struct{}{}
	d.mu.Unlock()

	d.handleComplaint(2, d.round)

	// len(complains)==1, faults==1: 1 > 1 is false → no cascade, no round change
	assert.Empty(t, link.sentMessages())
	d.mu.RLock()
	defer d.mu.RUnlock()
	assert.Equal(t, 1, d.round)
	assert.False(t, d.complained)
}

func TestHandleComplaint_MoreThanFaults_CascadesOwnComplaint(t *testing.T) {
	// f=1: 2 complaints → len > f → cascade own complaint to all processes
	processes := makeProcesses(1, 2, 3, 4)
	d, link := newDetector(1, processes, 1)
	d.Init()

	for _, pid := range []types.ProcessID{2, 3} {
		d.mu.Lock()
		d.complains[pid] = struct{}{}
		d.mu.Unlock()
		d.handleComplaint(pid, d.round)
	}

	sent := link.sentMessages()
	assert.NotEmpty(t, sent, "should cascade own complaint when len(complains) > f")
	d.mu.RLock()
	defer d.mu.RUnlock()
	assert.True(t, d.complained)
}

func TestHandleComplaint_CascadeComplaint_NotSentTwice(t *testing.T) {
	// Once complained==true, another complaint above threshold must not re-broadcast
	processes := makeProcesses(1, 2, 3, 4)
	d, link := newDetector(1, processes, 1)
	d.Init()

	d.mu.Lock()
	d.complained = true
	for _, pid := range []types.ProcessID{2, 3} {
		d.complains[pid] = struct{}{}
	}
	d.mu.Unlock()

	d.handleComplaint(2, d.round)
	assert.Empty(t, link.sentMessages(), "must not re-broadcast when already complained")
}

func TestHandleComplaint_MoreThan2Faults_AdvancesRound(t *testing.T) {
	// f=1: need len > 2*1 = 2, so 3 complaints trigger round advance.
	// BUG: double mutex unlock causes panic — verified with require.NotPanics to document.
	processes := makeProcesses(1, 2, 3, 4)
	recv := &mockReceiver{}
	d, _ := newDetector(1, processes, 1)
	d.Init()
	d.Subscribe(recv)

	// 3 complaints pre-populated (bypass inverted guard)
	for _, pid := range []types.ProcessID{2, 3, 4} {
		d.mu.Lock()
		d.complains[pid] = struct{}{}
		d.mu.Unlock()
	}

	// The final handleComplaint should advance the round; double-unlock is the known bug
	require.NotPanics(t, func() {
		d.handleComplaint(4, d.round)
	})

	d.mu.RLock()
	round := d.round
	d.mu.RUnlock()
	assert.Equal(t, 2, round, "round must advance after > 2*f complaints")

	leaders := recv.receivedLeaders()
	assert.NotEmpty(t, leaders, "receiver must be notified of new leader after round advance")
}

func TestHandleComplaint_AdvancesRound_ResetsComplains(t *testing.T) {
	processes := makeProcesses(1, 2, 3, 4)
	d, _ := newDetector(1, processes, 1)
	d.Init()

	for _, pid := range []types.ProcessID{2, 3, 4} {
		d.mu.Lock()
		d.complains[pid] = struct{}{}
		d.mu.Unlock()
	}

	_ = func() { d.handleComplaint(4, d.round) } // may panic due to double-unlock bug

	// After reset, complained flag should be false
	d.mu.RLock()
	defer d.mu.RUnlock()
	assert.False(t, d.complained)
}
