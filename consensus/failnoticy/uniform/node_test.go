package uniform

import (
	"context"
	"reliable/broadcaster"
	"reliable/database/inmemory"
	"reliable/election"
	"reliable/messages"
	"reliable/p2p"
	"reliable/types"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ══════════════════════════════════════════════════════════════
// Mock EpochConsensus — полностью контролируемый из теста
// ══════════════════════════════════════════════════════════════

type mockEC struct {
	mu sync.Mutex

	epoch     int
	leader    types.ProcessID
	state     State
	started   bool
	aborted   bool
	proposed  *types.Value
	delivered []types.Message

	decidedCh chan types.Value
	abortedCh chan AbortedState

	onAbort func() // хук, вызываемый при Abort()
}

func newMockEC(epoch int) *mockEC {
	return &mockEC{
		epoch:     epoch,
		decidedCh: make(chan types.Value, 1),
		abortedCh: make(chan AbortedState, 1),
		delivered: make([]types.Message, 0),
	}
}

func (m *mockEC) StartEpoch(_ context.Context, leader types.ProcessID, epoch int, current State) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.epoch = epoch
	m.leader = leader
	m.state = current
	m.started = true
}

func (m *mockEC) Epoch() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.epoch
}

func (m *mockEC) Propose(v types.Value) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := v.Copy()
	m.proposed = &cp
}

func (m *mockEC) Abort() {
	m.mu.Lock()
	alreadyAborted := m.aborted
	m.aborted = true
	hook := m.onAbort
	m.mu.Unlock()

	if alreadyAborted {
		return
	}
	if hook != nil {
		hook()
		return
	}
	// По умолчанию: шлём aborted state и закрываем каналы (как реальный EC)
	m.abortedCh <- AbortedState{Ts: m.epoch, State: &State{}}
	close(m.abortedCh)
	close(m.decidedCh)
}

func (m *mockEC) Aborted() <-chan AbortedState {
	return m.abortedCh
}

func (m *mockEC) Decided() chan types.Value {
	return m.decidedCh
}

func (m *mockEC) Deliver(msg types.Message) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.delivered = append(m.delivered, msg)
}

func (m *mockEC) isStarted() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.started
}

func (m *mockEC) isAborted() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.aborted
}

func (m *mockEC) getProposed() *types.Value {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.proposed
}

func (m *mockEC) getDelivered() []types.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]types.Message, len(m.delivered))
	copy(cp, m.delivered)
	return cp
}

func (m *mockEC) getLeader() types.ProcessID {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.leader
}

func (m *mockEC) getState() State {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.state
}

// emitDecided — имитирует: EC решил значение
func (m *mockEC) emitDecided(val types.Value) {
	m.decidedCh <- val
	close(m.decidedCh)
	close(m.abortedCh)
}

// ══════════════════════════════════════════════════════════════
// Mock Broadcaster + Mock P2P Link — минимальные заглушки
// ══════════════════════════════════════════════════════════════

type mockBroadcaster struct {
	types.UnimplementedDeliverer
	pid types.ProcessID
}

func (b *mockBroadcaster) Broadcast(_ context.Context, _ types.Message)               {}
func (b *mockBroadcaster) AddDeliverer(_ types.Deliverer, _ ...types.DelivererOption) {}
func (b *mockBroadcaster) RemoveDeliverer(_ types.Deliverer)                          {}
func (b *mockBroadcaster) AddCorrect(_ types.ProcessID)                               {}
func (b *mockBroadcaster) RemoveCorrect(_ types.ProcessID)                            {}
func (b *mockBroadcaster) Init()                                                      {}
func (b *mockBroadcaster) Start()                                                     {}
func (b *mockBroadcaster) Stop()                                                      {}
func (b *mockBroadcaster) ProcessID() types.ProcessID                                 { return b.pid }

type mockLink struct {
	types.UnimplementedDeliverer
	pid types.ProcessID
}

func (l *mockLink) Send(_ types.ProcessID, _ types.Message)                    {}
func (l *mockLink) AddDeliverer(_ types.Deliverer, _ ...types.DelivererOption) {}
func (l *mockLink) RemoveDeliverer(_ types.Deliverer)                          {}
func (l *mockLink) Init()                                                      {}
func (l *mockLink) Start()                                                     {}
func (l *mockLink) Stop()                                                      {}
func (l *mockLink) ProcessID() types.ProcessID                                 { return l.pid }

// ══════════════════════════════════════════════════════════════
// Mock LeaderBasedEpochChanger — нужен конструктору Node
// ══════════════════════════════════════════════════════════════

func newMockEpochChanger(ctx context.Context, self types.ProcessID) *LeaderBasedEpochChanger {
	processes := map[types.ProcessID]types.ProcessRank{
		self: types.ProcessRank(int(self)),
	}
	beb := &mockBroadcaster{}
	pl := &mockLink{}
	rt := types.NewRuntime()
	ec := NewLeaderBasedEpochChanger(ctx, self, processes, beb, election.NewLowerEpochElection(
		ctx, self, processes, inmemory.NewKVStore(), pl, 100*time.Millisecond, nil,
	), pl, rt)
	return ec
}

// ══════════════════════════════════════════════════════════════
// Хелпер: создаёт Node с мок-фабрикой
// ══════════════════════════════════════════════════════════════

// ecTracker собирает все созданные фабрикой mock EC
type ecTracker struct {
	mu      sync.Mutex
	created []*mockEC
	factory func(epoch int) *mockEC // кастомная фабрика, если нужна
}

func newECTracker() *ecTracker {
	return &ecTracker{
		created: make([]*mockEC, 0),
	}
}

func (t *ecTracker) last() *mockEC {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.created) == 0 {
		return nil
	}
	return t.created[len(t.created)-1]
}

func (t *ecTracker) all() []*mockEC {
	t.mu.Lock()
	defer t.mu.Unlock()
	cp := make([]*mockEC, len(t.created))
	copy(cp, t.created)
	return cp
}

func (t *ecTracker) count() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.created)
}

func makeTestNode(
	ctx context.Context,
	self types.ProcessID,
	processes map[types.ProcessID]types.ProcessRank,
	tracker *ecTracker,
) *Node {
	beb := &mockBroadcaster{}
	pl := &mockLink{}
	changer := newMockEpochChanger(ctx, self)

	n := New(ctx, self, processes, changer, pl, beb)
	n.logger = zerolog.Nop()

	// Подменяем фабрику
	n.ecFactory = func(
		selfID types.ProcessID,
		_ broadcaster.Broadcaster,
		_ p2p.Link,
		pcCount int,
		_ *zerolog.Logger,
	) EpochConsensus {
		tracker.mu.Lock()
		defer tracker.mu.Unlock()

		var mock *mockEC
		if tracker.factory != nil {
			mock = tracker.factory(0)
		} else {
			mock = newMockEC(0)
		}
		tracker.created = append(tracker.created, mock)
		return mock
	}

	return n
}

func defaultProcesses(ids ...types.ProcessID) map[types.ProcessID]types.ProcessRank {
	m := make(map[types.ProcessID]types.ProcessRank, len(ids))
	for _, id := range ids {
		m[id] = types.ProcessRank(int(id))
	}
	return m
}

const testTimeout = 3 * time.Second

// ══════════════════════════════════════════════════════════════
// ТЕСТЫ
// ══════════════════════════════════════════════════════════════

// ──────────────────────────────────────────────────────────────
// 1. Init + Start: Node создаёт EC через фабрику, вызывает StartEpoch
// ──────────────────────────────────────────────────────────────

func TestNode_Start_CreatesEpochConsensus(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	self := types.ProcessID(1)
	procs := defaultProcesses(1, 2, 3)
	tracker := newECTracker()

	n := makeTestNode(ctx, self, procs, tracker)
	n.Init()
	n.Start()
	defer n.Stop()

	// Даём background() запуститься
	time.Sleep(50 * time.Millisecond)

	require.Equal(t, 1, tracker.count(), "фабрика должна быть вызвана ровно 1 раз при Start")

	ec := tracker.last()
	require.NotNil(t, ec)
	assert.True(t, ec.isStarted(), "EC должен быть StartEpoch'd")
}

// ──────────────────────────────────────────────────────────────
// 2. Init определяет лидера как процесс с минимальным rank
// ──────────────────────────────────────────────────────────────

func TestNode_Init_LeaderIsMinRank(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	// pid=3 имеет rank=1 (минимальный) → должен стать лидером
	procs := map[types.ProcessID]types.ProcessRank{
		1: 10,
		2: 5,
		3: 1,
	}
	self := types.ProcessID(1)
	tracker := newECTracker()

	n := makeTestNode(ctx, self, procs, tracker)
	n.Init()
	n.Start()
	defer n.Stop()

	time.Sleep(50 * time.Millisecond)

	ec := tracker.last()
	require.NotNil(t, ec)
	assert.Equal(t, types.ProcessID(3), ec.getLeader(),
		"лидером должен стать процесс с наименьшим rank")
}

// ──────────────────────────────────────────────────────────────
// 3. Propose сохраняет значение, лидер пробрасывает в inner
// ──────────────────────────────────────────────────────────────

func TestNode_Propose_LeaderProposesToInnerEC(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	self := types.ProcessID(1)
	procs := defaultProcesses(1, 2, 3) // rank=pid, min=1 → self = leader
	tracker := newECTracker()

	n := makeTestNode(ctx, self, procs, tracker)
	n.Init()
	n.Start()
	defer n.Stop()

	val := types.IntValue(42)
	n.Propose(val)

	// maybePropose вызывается по таймеру (~200мс первый раз, потом ~100мс)
	time.Sleep(400 * time.Millisecond)

	ec := tracker.last()
	require.NotNil(t, ec)

	proposed := ec.getProposed()
	require.NotNil(t, proposed, "inner EC должен получить Propose")
	assert.True(t, (*proposed).Compare(val), "proposed value должно совпадать")
}

// ──────────────────────────────────────────────────────────────
// 4. Propose НЕ пробрасывается, если self != leader
// ──────────────────────────────────────────────────────────────

func TestNode_Propose_FollowerDoesNotProposeToInner(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	// pid=2 → rank=2, pid=1 → rank=1 → leader=1 → self(2) != leader
	self := types.ProcessID(2)
	procs := defaultProcesses(1, 2, 3)
	tracker := newECTracker()

	n := makeTestNode(ctx, self, procs, tracker)
	n.Init()
	n.Start()
	defer n.Stop()

	n.Propose(types.IntValue(99))

	time.Sleep(400 * time.Millisecond)

	ec := tracker.last()
	require.NotNil(t, ec)
	assert.Nil(t, ec.getProposed(), "фолловер не должен пробрасывать Propose в inner EC")
}

// ──────────────────────────────────────────────────────────────
// 5. Decided: inner EC решает → Node отдаёт значение в Decided()
// ──────────────────────────────────────────────────────────────

func TestNode_Decided_PropagatesFromInnerEC(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	self := types.ProcessID(1)
	procs := defaultProcesses(1, 2, 3)
	tracker := newECTracker()

	n := makeTestNode(ctx, self, procs, tracker)
	n.Init()
	n.Start()
	defer n.Stop()

	time.Sleep(50 * time.Millisecond)

	ec := tracker.last()
	require.NotNil(t, ec)

	val := types.IntValue(777)
	ec.emitDecided(val)

	select {
	case decided := <-n.Decided():
		assert.True(t, decided.Compare(val), "Node.Decided() должен отдать значение из inner EC")
	case <-time.After(testTimeout):
		t.Fatal("timeout: Node.Decided() не отдал значение")
	}
}

// ──────────────────────────────────────────────────────────────
// 6. Decided закрывает канал — повторное чтение не блокирует
// ──────────────────────────────────────────────────────────────

func TestNode_Decided_ChannelClosedAfterValue(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	self := types.ProcessID(1)
	procs := defaultProcesses(1, 2, 3)
	tracker := newECTracker()

	n := makeTestNode(ctx, self, procs, tracker)
	n.Init()
	n.Start()
	defer n.Stop()

	time.Sleep(50 * time.Millisecond)

	ec := tracker.last()
	require.NotNil(t, ec)

	ec.emitDecided(types.IntValue(1))

	// Первое чтение — значение
	select {
	case <-n.Decided():
	case <-time.After(testTimeout):
		t.Fatal("timeout первого чтения")
	}

	// Второе чтение — канал закрыт (close(n.decideCh) в background)
	select {
	case _, ok := <-n.Decided():
		assert.False(t, ok, "decideCh должен быть закрыт после решения")
	case <-time.After(testTimeout):
		t.Fatal("timeout: decideCh не закрыт")
	}
}

// ──────────────────────────────────────────────────────────────
// 7. NewEpoch: epoch changer триггерит новую эпоху →
//    старый inner абортится, создаётся новый
// ──────────────────────────────────────────────────────────────

func TestNode_NewEpoch_AbortsOldAndCreatesNew(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	self := types.ProcessID(1)
	procs := defaultProcesses(1, 2, 3)
	tracker := newECTracker()

	n := makeTestNode(ctx, self, procs, tracker)
	n.Init()
	n.Start()
	defer n.Stop()

	time.Sleep(50 * time.Millisecond)

	oldEC := tracker.last()
	require.NotNil(t, oldEC)
	require.Equal(t, 1, tracker.count())

	// Триггерим новую эпоху (ts > ets=0)
	n.StartEpoch(5, types.ProcessID(2))

	// Ждём: old EC абортится → aborted state → новый EC создаётся
	time.Sleep(200 * time.Millisecond)

	assert.True(t, oldEC.isAborted(), "старый inner EC должен быть Abort'd")
	assert.Equal(t, 2, tracker.count(), "должен быть создан новый inner EC")

	newEC := tracker.last()
	require.NotNil(t, newEC)
	assert.True(t, newEC.isStarted(), "новый EC должен быть StartEpoch'd")
	assert.Equal(t, types.ProcessID(2), newEC.getLeader(), "лидер новой эпохи = 2")
}

// ──────────────────────────────────────────────────────────────
// 8. NewEpoch с ts <= ets игнорируется
// ──────────────────────────────────────────────────────────────

func TestNode_NewEpoch_OldTsIgnored(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	self := types.ProcessID(1)
	procs := defaultProcesses(1, 2, 3)
	tracker := newECTracker()

	n := makeTestNode(ctx, self, procs, tracker)
	n.Init()
	n.Start()
	defer n.Stop()

	time.Sleep(50 * time.Millisecond)
	require.Equal(t, 1, tracker.count())

	// Первая эпоха → ts=5
	n.StartEpoch(5, types.ProcessID(2))
	time.Sleep(200 * time.Millisecond)
	require.Equal(t, 2, tracker.count())

	// Шлём эпоху с ts=3 (меньше текущего ets=5) → должна быть проигнорирована
	n.StartEpoch(3, types.ProcessID(3))
	time.Sleep(200 * time.Millisecond)

	assert.Equal(t, 2, tracker.count(),
		"эпоха с ts <= ets не должна создавать новый inner EC")
}

// ──────────────────────────────────────────────────────────────
// 9. Deliver: сообщение пробрасывается в inner.Deliver
// ──────────────────────────────────────────────────────────────

func TestNode_Deliver_ForwardsToInner(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	self := types.ProcessID(1)
	procs := defaultProcesses(1, 2, 3)
	tracker := newECTracker()

	n := makeTestNode(ctx, self, procs, tracker)
	n.Init()
	n.Start()
	defer n.Stop()

	time.Sleep(50 * time.Millisecond)

	ec := tracker.last()
	require.NotNil(t, ec)

	// Шлём ReadMsg через Deliver (как будто beb доставил)
	msg := ReadMsg{
		Message: messages.NewBase(uuid.New(), types.ProcessID(2), ReadMsgName),
		Ts:      0,
	}
	n.Deliver(msg)

	time.Sleep(100 * time.Millisecond)

	delivered := ec.getDelivered()
	require.Len(t, delivered, 1, "inner EC должен получить ровно 1 сообщение")

	_, ok := delivered[0].(ReadMsg)
	assert.True(t, ok, "доставленное сообщение должно быть ReadMsg")
}

// ──────────────────────────────────────────────────────────────
// 10. Deliver при inner==nil → сообщение буферизуется в inbox
// ──────────────────────────────────────────────────────────────

func TestNode_Deliver_BuffersWhenInnerNil(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	self := types.ProcessID(1)
	procs := defaultProcesses(1, 2, 3)

	abortBlock := make(chan struct{})
	tracker := newECTracker()
	tracker.factory = func(epoch int) *mockEC {
		m := newMockEC(epoch)
		// Первый EC: при Abort блокируемся, чтобы создать окно inner==nil
		if tracker.count() == 0 {
			m.onAbort = func() {
				<-abortBlock
				m.abortedCh <- AbortedState{Ts: m.epoch, State: &State{}}
				close(m.abortedCh)
				close(m.decidedCh)
			}
		}
		return m
	}

	n := makeTestNode(ctx, self, procs, tracker)
	n.Init()
	n.Start()
	defer n.Stop()

	time.Sleep(50 * time.Millisecond)
	require.Equal(t, 1, tracker.count())

	// Триггерим новую эпоху → inner станет nil (Abort заблокирован)
	n.StartEpoch(5, types.ProcessID(2))
	time.Sleep(100 * time.Millisecond)

	// Шлём сообщение пока inner==nil → должно попасть в inbox
	msg := WriteMsg{
		Message: messages.NewBase(uuid.New(), types.ProcessID(2), WriteMsgName),
		Val:     nil,
		Ts:      5,
	}
	n.Deliver(msg)
	time.Sleep(50 * time.Millisecond)

	// Разблокируем Abort → создастся новый EC → буферизованные сообщения доставятся
	close(abortBlock)
	time.Sleep(200 * time.Millisecond)

	require.Equal(t, 2, tracker.count())
	newEC := tracker.last()
	delivered := newEC.getDelivered()
	require.GreaterOrEqual(t, len(delivered), 1, 0)
}
