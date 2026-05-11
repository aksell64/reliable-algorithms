package byzantine

import (
	"context"
	"reliable/messages"
	"reliable/types"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

// mockStarter records StartEpoch calls.
type mockStarter struct {
	mu     sync.Mutex
	calls  []startCall
	callCh chan startCall
}

type startCall struct {
	ts     int
	leader types.ProcessID
}

func newMockStarter() *mockStarter {
	return &mockStarter{
		calls:  make([]startCall, 0),
		callCh: make(chan startCall, 10),
	}
}

func (s *mockStarter) StartEpoch(ts int, leader types.ProcessID) {
	s.mu.Lock()
	s.calls = append(s.calls, startCall{ts, leader})
	s.mu.Unlock()
	s.callCh <- startCall{ts, leader}
}

func (s *mockStarter) receivedCalls() []startCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]startCall, len(s.calls))
	copy(cp, s.calls)
	return cp
}

// mockMessage is a non-EpochMsg message for testing Deliver filtering.
type mockMessage struct {
	messages.BaseMsg
}

func (m mockMessage) Type() string { return "mock" }

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// fixedLeaderSelector always returns the same leader regardless of epoch.
func fixedLeaderSelector(leader types.ProcessID) LeaderSelector {
	return func(_ int, _ []types.ProcessID) types.ProcessID {
		return leader
	}
}

// rotatingLeaderSelector returns processes[epoch % len(processes)].
func rotatingLeaderSelector(processes []types.ProcessID) LeaderSelector {
	return func(epoch int, _ []types.ProcessID) types.ProcessID {
		return processes[epoch%len(processes)]
	}
}

// makeProcessIDs returns n process IDs starting from 1.
func makeProcessIDs(n int) []types.ProcessID {
	ids := make([]types.ProcessID, n)
	for i := range ids {
		ids[i] = types.ProcessID(i + 1)
	}
	return ids
}

// newTestEC creates an EpochChanger with mocks ready but NOT started.
func newTestEC(
	t *testing.T,
	self types.ProcessID,
	processes []types.ProcessID,
	faults int,
	selector LeaderSelector,
) (*EpochChanger, *mockLink) {
	t.Helper()
	link := newMockLink(self)
	ctx := context.Background()
	ec := NewEpochChanger(ctx, self, link, processes, faults, selector)
	return ec, link
}

// newStartedEC creates, inits and starts an EpochChanger; registers cleanup.
func newStartedEC(
	t *testing.T,
	self types.ProcessID,
	processes []types.ProcessID,
	faults int,
	selector LeaderSelector,
	starter *mockStarter,
) (*EpochChanger, *mockLink) {
	t.Helper()
	ec, link := newTestEC(t, self, processes, faults, selector)
	ec.SetStarter(starter)
	ec.Init()
	ec.Start()
	t.Cleanup(func() { ec.Stop() })
	return ec, link
}

// ---------------------------------------------------------------------------
// constructor
// ---------------------------------------------------------------------------

func TestNewEpochChanger_RegistersWithLink(t *testing.T) {
	processes := makeProcessIDs(4)
	_, link := newTestEC(t, 1, processes, 1, fixedLeaderSelector(1))
	assert.NotEmpty(t, link.deliverers, "EC must register itself with the link")
}

func TestNewEpochChanger_InitialEpochZero(t *testing.T) {
	processes := makeProcessIDs(4)
	ec, _ := newTestEC(t, 1, processes, 1, fixedLeaderSelector(1))
	assert.Equal(t, 0, ec.lastEpoch)
	assert.Equal(t, 0, ec.nextEpoch)
}

func TestNewEpochChanger_StoresParameters(t *testing.T) {
	processes := makeProcessIDs(4)
	ec, _ := newTestEC(t, 2, processes, 1, fixedLeaderSelector(1))
	assert.Equal(t, types.ProcessID(2), ec.self)
	assert.Equal(t, 1, ec.faults)
	assert.Equal(t, processes, ec.processes)
}

func TestNewEpochChanger_EmptyNewEpoches(t *testing.T) {
	processes := makeProcessIDs(4)
	ec, _ := newTestEC(t, 1, processes, 1, fixedLeaderSelector(1))
	assert.Empty(t, ec.newEpoches)
}

// ---------------------------------------------------------------------------
// Init
// ---------------------------------------------------------------------------

func TestInit_SetsLastEpochToOne(t *testing.T) {
	processes := makeProcessIDs(4)
	ec, _ := newTestEC(t, 1, processes, 1, fixedLeaderSelector(1))
	ec.Init()
	assert.Equal(t, 1, ec.lastEpoch)
}

func TestInit_NextEpochStillZero(t *testing.T) {
	// nextEpoch is only set when a complaint is raised.
	processes := makeProcessIDs(4)
	ec, _ := newTestEC(t, 1, processes, 1, fixedLeaderSelector(1))
	ec.Init()
	assert.Equal(t, 0, ec.nextEpoch)
}

func TestInit_SendsInitialTrustToBackground(t *testing.T) {
	// After Init() the trustedCh must carry the initial leader so that
	// background() picks it up when started.
	processes := makeProcessIDs(4)
	leader := types.ProcessID(1)
	ec, _ := newTestEC(t, 1, processes, 1, fixedLeaderSelector(leader))
	ec.Init()

	select {
	case got := <-ec.trustedCh:
		assert.Equal(t, leader, got)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("trustedCh must contain the initial leader after Init()")
	}
}

// ---------------------------------------------------------------------------
// checkComplaint (synchronous)
// ---------------------------------------------------------------------------

func TestCheckComplaint_TrustedMatchesExpected_NoBroadcast(t *testing.T) {
	processes := makeProcessIDs(4)
	leader := types.ProcessID(1)
	ec, link := newTestEC(t, 1, processes, 1, fixedLeaderSelector(leader))
	ec.lastEpoch = 1
	ec.nextEpoch = 1
	ec.trusted = leader // trusted == expected leader

	ec.checkComplaint()

	assert.Empty(t, link.sentMessages())
	assert.Equal(t, 1, ec.nextEpoch, "nextEpoch must not change when no complaint")
}

func TestCheckComplaint_TrustedMismatch_BroadcastsNewEpoch(t *testing.T) {
	processes := makeProcessIDs(4)
	leader := types.ProcessID(1)
	ec, link := newTestEC(t, 1, processes, 1, fixedLeaderSelector(leader))
	ec.lastEpoch = 1
	ec.nextEpoch = 1
	ec.trusted = types.ProcessID(2) // different from expected leader

	ec.checkComplaint()

	sent := link.sentMessages()
	assert.Len(t, sent, len(processes), "must broadcast NEWEPOCH to every process")
}

func TestCheckComplaint_TrustedMismatch_IncrementsNextEpoch(t *testing.T) {
	processes := makeProcessIDs(4)
	leader := types.ProcessID(1)
	ec, link := newTestEC(t, 1, processes, 1, fixedLeaderSelector(leader))
	ec.lastEpoch = 1
	ec.nextEpoch = 1
	ec.trusted = types.ProcessID(2)

	_ = link
	ec.checkComplaint()

	assert.Equal(t, 2, ec.nextEpoch)
}

func TestCheckComplaint_SentMessagesAreEpochMsg(t *testing.T) {
	processes := makeProcessIDs(4)
	leader := types.ProcessID(1)
	ec, link := newTestEC(t, 1, processes, 1, fixedLeaderSelector(leader))
	ec.lastEpoch = 1
	ec.nextEpoch = 1
	ec.trusted = types.ProcessID(2)

	ec.checkComplaint()

	for _, rec := range link.sentMessages() {
		emsg, ok := rec.msg.(EpochMsg)
		require.True(t, ok, "sent message must be EpochMsg")
		assert.Equal(t, 2, emsg.Epoch, "NEWEPOCH must carry lastEpoch+1")
	}
}

func TestCheckComplaint_Idempotent_NoDoubleBroadcast(t *testing.T) {
	// nextEpoch > lastEpoch means complaint was already raised.
	processes := makeProcessIDs(4)
	leader := types.ProcessID(1)
	ec, link := newTestEC(t, 1, processes, 1, fixedLeaderSelector(leader))
	ec.lastEpoch = 1
	ec.nextEpoch = 2 // already complained
	ec.trusted = types.ProcessID(2)

	ec.checkComplaint()

	assert.Empty(t, link.sentMessages(), "must not re-broadcast when nextEpoch > lastEpoch")
}

// ---------------------------------------------------------------------------
// checkQuorums (synchronous)
// ---------------------------------------------------------------------------

func TestCheckQuorums_InsufficientMessages_NoAction(t *testing.T) {
	// f=1: need >1 messages to join, so 1 message is not enough.
	processes := makeProcessIDs(4)
	starter := newMockStarter()
	ec, link := newTestEC(t, 1, processes, 1, fixedLeaderSelector(1))
	ec.SetStarter(starter)
	ec.lastEpoch = 1
	ec.nextEpoch = 1
	ec.newEpoches[types.ProcessID(2)] = struct{}{}

	ec.checkQuorums()

	assert.Empty(t, link.sentMessages())
	assert.Empty(t, starter.receivedCalls())
	assert.Equal(t, 1, ec.lastEpoch)
}

func TestCheckQuorums_FPlus1Messages_JoinsEpochChange(t *testing.T) {
	// f=1: 2 messages → count > f → broadcast NEWEPOCH (join)
	processes := makeProcessIDs(4)
	starter := newMockStarter()
	ec, link := newTestEC(t, 1, processes, 1, fixedLeaderSelector(1))
	ec.SetStarter(starter)
	ec.lastEpoch = 1
	ec.nextEpoch = 1
	ec.newEpoches[types.ProcessID(2)] = struct{}{}
	ec.newEpoches[types.ProcessID(3)] = struct{}{}

	ec.checkQuorums()

	assert.Len(t, link.sentMessages(), len(processes), "must broadcast NEWEPOCH to all on f+1 quorum")
	assert.Equal(t, 2, ec.nextEpoch)
	assert.Empty(t, starter.receivedCalls(), "StartEpoch must not be called on f+1 quorum")
}

func TestCheckQuorums_FPlus1_AlreadyComplained_NoBroadcast(t *testing.T) {
	// If nextEpoch already > lastEpoch, the f+1 branch must be skipped.
	processes := makeProcessIDs(4)
	starter := newMockStarter()
	ec, link := newTestEC(t, 1, processes, 1, fixedLeaderSelector(1))
	ec.SetStarter(starter)
	ec.lastEpoch = 1
	ec.nextEpoch = 2 // already complained
	ec.newEpoches[types.ProcessID(2)] = struct{}{}
	ec.newEpoches[types.ProcessID(3)] = struct{}{}

	ec.checkQuorums()

	assert.Empty(t, link.sentMessages(), "must not broadcast again when already complained")
}

func TestCheckQuorums_TwoFPlus1Messages_StartsNewEpoch(t *testing.T) {
	// f=1: 3 messages → count > 2*f → StartEpoch
	processes := makeProcessIDs(4)
	starter := newMockStarter()
	leader := processes[0]
	ec, _ := newTestEC(t, 1, processes, 1, fixedLeaderSelector(leader))
	ec.SetStarter(starter)
	ec.lastEpoch = 1
	ec.nextEpoch = 2 // must be > lastEpoch for the 2f+1 branch
	ec.newEpoches[types.ProcessID(2)] = struct{}{}
	ec.newEpoches[types.ProcessID(3)] = struct{}{}
	ec.newEpoches[types.ProcessID(4)] = struct{}{}

	ec.checkQuorums()

	calls := starter.receivedCalls()
	require.Len(t, calls, 1, "StartEpoch must be called exactly once on 2f+1 quorum")
	assert.Equal(t, 2, calls[0].ts)
	assert.Equal(t, leader, calls[0].leader)
}

func TestCheckQuorums_TwoFPlus1_AdvancesLastEpoch(t *testing.T) {
	processes := makeProcessIDs(4)
	starter := newMockStarter()
	ec, _ := newTestEC(t, 1, processes, 1, fixedLeaderSelector(1))
	ec.SetStarter(starter)
	ec.lastEpoch = 1
	ec.nextEpoch = 2
	ec.newEpoches[types.ProcessID(2)] = struct{}{}
	ec.newEpoches[types.ProcessID(3)] = struct{}{}
	ec.newEpoches[types.ProcessID(4)] = struct{}{}

	ec.checkQuorums()

	assert.Equal(t, 2, ec.lastEpoch, "lastEpoch must advance after 2f+1 quorum")
}

func TestCheckQuorums_TwoFPlus1_ResetsNewEpoches(t *testing.T) {
	processes := makeProcessIDs(4)
	starter := newMockStarter()
	ec, _ := newTestEC(t, 1, processes, 1, fixedLeaderSelector(1))
	ec.SetStarter(starter)
	ec.lastEpoch = 1
	ec.nextEpoch = 2
	ec.newEpoches[types.ProcessID(2)] = struct{}{}
	ec.newEpoches[types.ProcessID(3)] = struct{}{}
	ec.newEpoches[types.ProcessID(4)] = struct{}{}

	ec.checkQuorums()

	assert.Empty(t, ec.newEpoches, "newEpoches must be reset after 2f+1 quorum")
}

// ---------------------------------------------------------------------------
// handleNewEpoch (synchronous)
// ---------------------------------------------------------------------------

func TestHandleNewEpoch_CorrectEpoch_Recorded(t *testing.T) {
	processes := makeProcessIDs(4)
	ec, _ := newTestEC(t, 1, processes, 1, fixedLeaderSelector(1))
	ec.lastEpoch = 1

	ec.handleNewEpoch(types.ProcessID(2), 2)

	_, ok := ec.newEpoches[types.ProcessID(2)]
	assert.True(t, ok, "process must be recorded in newEpoches on correct epoch")
}

func TestHandleNewEpoch_WrongEpoch_Ignored(t *testing.T) {
	processes := makeProcessIDs(4)
	ec, _ := newTestEC(t, 1, processes, 1, fixedLeaderSelector(1))
	ec.lastEpoch = 1

	ec.handleNewEpoch(types.ProcessID(2), 99) // wrong epoch

	assert.Empty(t, ec.newEpoches, "wrong-epoch message must not be recorded")
}

func TestHandleNewEpoch_PastEpoch_Ignored(t *testing.T) {
	processes := makeProcessIDs(4)
	ec, _ := newTestEC(t, 1, processes, 1, fixedLeaderSelector(1))
	ec.lastEpoch = 3

	ec.handleNewEpoch(types.ProcessID(2), 2) // lastEpoch+1 = 4, not 2

	assert.Empty(t, ec.newEpoches)
}

func TestHandleNewEpoch_Deduplication(t *testing.T) {
	// Same process sending twice must only count once.
	processes := makeProcessIDs(4)
	ec, _ := newTestEC(t, 1, processes, 1, fixedLeaderSelector(1))
	ec.lastEpoch = 1

	ec.handleNewEpoch(types.ProcessID(2), 2)
	ec.handleNewEpoch(types.ProcessID(2), 2)

	assert.Len(t, ec.newEpoches, 1)
}

// ---------------------------------------------------------------------------
// broadcastNewEpoch (synchronous)
// ---------------------------------------------------------------------------

func TestBroadcastNewEpoch_SendsToAllProcesses(t *testing.T) {
	processes := makeProcessIDs(4)
	ec, link := newTestEC(t, 1, processes, 1, fixedLeaderSelector(1))
	ec.nextEpoch = 2

	ec.broadcastNewEpoch()

	sent := link.sentMessages()
	assert.Len(t, sent, len(processes))

	recipients := make(map[types.ProcessID]bool)
	for _, rec := range sent {
		recipients[rec.to] = true
	}
	for _, pid := range processes {
		assert.True(t, recipients[pid], "NEWEPOCH must be sent to process %d", pid)
	}
}

func TestBroadcastNewEpoch_MessageCarriesCorrectEpoch(t *testing.T) {
	processes := makeProcessIDs(4)
	ec, link := newTestEC(t, 1, processes, 1, fixedLeaderSelector(1))
	ec.nextEpoch = 3

	ec.broadcastNewEpoch()

	for _, rec := range link.sentMessages() {
		emsg, ok := rec.msg.(EpochMsg)
		require.True(t, ok)
		assert.Equal(t, 3, emsg.Epoch)
	}
}

func TestBroadcastNewEpoch_MessageFromSelf(t *testing.T) {
	processes := makeProcessIDs(4)
	self := types.ProcessID(2)
	ec, link := newTestEC(t, self, processes, 1, fixedLeaderSelector(1))
	ec.nextEpoch = 2

	ec.broadcastNewEpoch()

	for _, rec := range link.sentMessages() {
		emsg, ok := rec.msg.(EpochMsg)
		require.True(t, ok)
		assert.Equal(t, self, emsg.From())
	}
}

// ---------------------------------------------------------------------------
// Deliver
// ---------------------------------------------------------------------------

func TestDeliver_NonEpochMsg_Ignored(t *testing.T) {
	processes := makeProcessIDs(4)
	ec, _ := newTestEC(t, 1, processes, 1, fixedLeaderSelector(1))
	ec.lastEpoch = 1

	other := mockMessage{BaseMsg: messages.NewBase(uuid.New(), 2, "mock")}
	ec.Deliver(other)

	// Nothing should be queued in newEpochesCh.
	select {
	case <-ec.newEpochesCh:
		t.Fatal("non-EpochMsg must not be forwarded to newEpochesCh")
	case <-time.After(50 * time.Millisecond):
		// correct: nothing delivered
	}
}

func TestDeliver_EpochMsg_ForwardsToChannel(t *testing.T) {
	processes := makeProcessIDs(4)
	ec, _ := newTestEC(t, 1, processes, 1, fixedLeaderSelector(1))
	ec.lastEpoch = 1

	from := types.ProcessID(2)
	msg := NewEpochMsg(from, 2)
	ec.Deliver(msg)

	select {
	case env := <-ec.newEpochesCh:
		assert.Equal(t, from, env.from)
		assert.Equal(t, 2, env.epoch)
	case <-time.After(200 * time.Millisecond):
		t.Fatal("EpochMsg must be forwarded to newEpochesCh")
	}
}

// ---------------------------------------------------------------------------
// Integration: async background loop
// ---------------------------------------------------------------------------

func TestIntegration_ComplaintBroadcastsNewEpoch(t *testing.T) {
	// When the trusted leader differs from the expected leader,
	// the background loop must broadcast NEWEPOCH to all processes.
	t.Parallel()

	processes := makeProcessIDs(4)
	leader := processes[0] // process 1 is always the expected leader
	wrongLeader := processes[1]

	starter := newMockStarter()
	ec, link := newStartedEC(t, 1, processes, 1, fixedLeaderSelector(leader), starter)

	// Drain the initial trust notification from Init.
	<-time.After(50 * time.Millisecond)
	link.clearSent()

	// Notify EC that a DIFFERENT process is now trusted (triggers complaint).
	ec.OnNewLeader(wrongLeader)

	require.Eventually(t, func() bool {
		return len(link.sentMessages()) >= len(processes)
	}, 500*time.Millisecond, 10*time.Millisecond, "complaint must broadcast NEWEPOCH to all processes")
}

func TestIntegration_TwoFPlus1QuorumStartsNewEpoch(t *testing.T) {
	// When 2f+1 NEWEPOCH messages arrive, StartEpoch must be called.
	t.Parallel()

	processes := makeProcessIDs(4) // f=1 → need >2 messages
	leader := processes[0]

	starter := newMockStarter()
	ec, _ := newStartedEC(t, 1, processes, 1, fixedLeaderSelector(leader), starter)

	// Set nextEpoch > lastEpoch to satisfy 2f+1 branch precondition.
	// We simulate the complaint path by delivering f+1 messages first,
	// then the full 2f+1 set.
	for _, pid := range []types.ProcessID{2, 3, 4} {
		msg := NewEpochMsg(pid, 2)
		ec.Deliver(msg)
	}

	select {
	case call := <-starter.callCh:
		assert.Equal(t, 2, call.ts)
		assert.Equal(t, leader, call.leader)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("StartEpoch must be called after 2f+1 NEWEPOCH messages")
	}
}

func TestIntegration_StaleEpochMessagesIgnored(t *testing.T) {
	// Messages carrying a wrong epoch number must be silently dropped.
	t.Parallel()

	processes := makeProcessIDs(4)
	starter := newMockStarter()
	ec, _ := newStartedEC(t, 1, processes, 1, fixedLeaderSelector(1), starter)

	// Deliver messages with epoch 99 (expected is lastEpoch+1 = 2).
	for _, pid := range []types.ProcessID{2, 3, 4} {
		msg := NewEpochMsg(pid, 99)
		ec.Deliver(msg)
	}

	<-time.After(200 * time.Millisecond)

	assert.Empty(t, starter.receivedCalls(), "stale-epoch messages must not trigger StartEpoch")
}

func TestIntegration_OnNewLeader_SetsNewTrusted(t *testing.T) {
	t.Parallel()

	processes := makeProcessIDs(4)
	leader := processes[0]
	starter := newMockStarter()
	ec, _ := newStartedEC(t, 1, processes, 1, fixedLeaderSelector(leader), starter)

	// Wait for background to consume Init's initial trust.
	<-time.After(50 * time.Millisecond)

	newLeader := processes[0]
	ec.OnNewLeader(newLeader)

	require.Eventually(t, func() bool {
		return ec.trusted == newLeader
	}, 300*time.Millisecond, 5*time.Millisecond, "trusted must update after OnNewLeader")
}

func TestIntegration_FPlus1QuorumJoinsBeforeTwoFPlus1(t *testing.T) {
	// With f=1: 2 messages trigger the f+1 join (broadcast),
	// 3 messages then trigger the 2f+1 quorum (StartEpoch).
	t.Parallel()

	processes := makeProcessIDs(4)
	leader := processes[0]
	starter := newMockStarter()
	ec, link := newStartedEC(t, 1, processes, 1, fixedLeaderSelector(leader), starter)

	// Drain initial trust.
	<-time.After(50 * time.Millisecond)
	link.clearSent()

	// Trigger f+1 by delivering 2 NEWEPOCH messages.
	ec.Deliver(NewEpochMsg(2, 2))
	ec.Deliver(NewEpochMsg(3, 2))

	// EC must broadcast NEWEPOCH (join) without calling StartEpoch.
	require.Eventually(t, func() bool {
		return len(link.sentMessages()) >= len(processes)
	}, 300*time.Millisecond, 5*time.Millisecond, "EC must broadcast NEWEPOCH on f+1 quorum")

	assert.Empty(t, starter.receivedCalls(), "StartEpoch must not be called on f+1 quorum alone")

	// Now deliver the 3rd message to reach 2f+1 and trigger StartEpoch.
	ec.Deliver(NewEpochMsg(4, 2))

	select {
	case <-starter.callCh:
		// OK
	case <-time.After(300 * time.Millisecond):
		t.Fatal("StartEpoch must be called after 2f+1 NEWEPOCH messages")
	}
}
