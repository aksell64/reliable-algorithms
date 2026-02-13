package uniform

import (
	"context"
	"reliable/messages"
	"reliable/types"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

// ==================== Моки ====================

type mockBroadcaster struct {
	types.Layer
	mu        sync.Mutex
	messages  []types.Message
	deliverer types.Deliverer
}

func newMockBroadcaster() *mockBroadcaster {
	return &mockBroadcaster{}
}

func (mb *mockBroadcaster) Broadcast(ctx context.Context, msg types.Message) {
	mb.mu.Lock()
	mb.messages = append(mb.messages, msg)
	d := mb.deliverer
	mb.mu.Unlock()

	// Имитируем доставку самому себе
	if d != nil {
		d.Deliver(msg)
	}
}

func (mb *mockBroadcaster) AddDeliverer(d types.Deliverer) {
	mb.mu.Lock()
	mb.deliverer = d
	mb.mu.Unlock()
}
func (mb *mockBroadcaster) RemoveDeliverer(d types.Deliverer) {
	mb.mu.Lock()
	mb.deliverer = nil
	mb.mu.Unlock()
}

func (mb *mockBroadcaster) getMessages() []types.Message {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	cp := make([]types.Message, len(mb.messages))
	copy(cp, mb.messages)
	return cp
}

type mockLink struct {
	mu        sync.Mutex
	sent      []sentMsg
	deliverer types.Deliverer
}

type sentMsg struct {
	to  types.ProcessID
	msg types.Message
}

func newMockLink() *mockLink {
	return &mockLink{}
}

func (ml *mockLink) Send(to types.ProcessID, msg types.Message) {
	ml.mu.Lock()
	ml.sent = append(ml.sent, sentMsg{to: to, msg: msg})
	d := ml.deliverer
	ml.mu.Unlock()

	// Доставляем обратно отправителю (имитация ответа лидеру)
	if d != nil {
		d.Deliver(msg)
	}
}

func (ml *mockLink) AddDeliverer(d types.Deliverer) { ml.mu.Lock(); ml.deliverer = d; ml.mu.Unlock() }
func (ml *mockLink) RemoveDeliverer(d types.Deliverer) {
	ml.mu.Lock()
	ml.deliverer = nil
	ml.mu.Unlock()
}

func (ml *mockLink) getSent() []sentMsg {
	ml.mu.Lock()
	defer ml.mu.Unlock()
	cp := make([]sentMsg, len(ml.sent))
	copy(cp, ml.sent)
	return cp
}

// ==================== Хелперы ====================

func waitForDecision(t *testing.T, decided <-chan types.Value, timeout time.Duration) types.Value {
	t.Helper()
	select {
	case v := <-decided:
		return v
	case <-time.After(timeout):
		t.Fatal("таймаут: решение не принято")
		return nil
	}
}

func assertPhase(t *testing.T, ec *epochConsensus, expected types.State) {
	t.Helper()
	// Даём event loop чуть-чуть времени обработать
	time.Sleep(50 * time.Millisecond)
	if ec.sm.Current() != expected {
		t.Errorf("ожидалась фаза %v, получена %v", expected, ec.sm.Current())
	}
}

// ==================== Тесты ====================

// Лидер после инициализации переходит в PhasePropose
func TestLeaderInitTransitionsToPropose(t *testing.T) {
	ctx := context.Background()
	beb := newMockBroadcaster()
	pl := newMockLink()

	leader := types.ProcessID(1)

	ec := newEpochConsensus(leader, beb, pl, 1)
	ec.StartEpoch(leader, 1, nil)
	defer ec.abort()

	assertPhase(t, ec, StatePropose)
}

// Не-лидер после инициализации остаётся в PhaseIdle
func TestFollowerInitStaysIdle(t *testing.T) {
	ctx := context.Background()
	beb := newMockBroadcaster()
	pl := newMockLink()

	self := types.ProcessID(2)
	leader := types.ProcessID(1)

	ec := newEpochConsensus(ctx, self, beb, pl, 3)
	ec.StartEpoch(leader, 1, nil)
	defer ec.abort()

	assertPhase(t, ec, StateIdle)
}

// Лидер: Propose → Read broadcast
func TestLeaderProposeBroadcastsRead(t *testing.T) {
	ctx := context.Background()
	beb := newMockBroadcaster()
	pl := newMockLink()

	leader := types.ProcessID(1)

	// Не доставляем себе — просто проверяем что broadcast произошёл
	ec := newEpochConsensus(ctx, leader, beb, pl, 3)
	beb.deliverer = nil // отключаем self-delivery чтобы не усложнять
	ec.StartEpoch(leader, 1, nil)
	defer ec.abort()

	val := types.IntValue(10)
	ec.Propose(val)

	time.Sleep(100 * time.Millisecond)

	msgs := beb.getMessages()
	found := false
	for _, m := range msgs {
		if _, ok := m.(ReadMsg); ok {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("лидер не отправил ReadMsg после Propose")
	}

	assertPhase(t, ec, StateReading)
}

// Slave: получает ReadMsg от лидера → отвечает StateMsg
func TestFollowerRespondsToReadWithState(t *testing.T) {
	ctx := context.Background()
	beb := newMockBroadcaster()
	pl := newMockLink()

	self := types.ProcessID(2)
	leader := types.ProcessID(1)

	pl.deliverer = nil // не доставляем ответ обратно

	ec := newEpochConsensus(ctx, self, beb, pl, 3)
	ec.StartEpoch(leader, 5, &state{ts: 3, val: ptrVal("old-val")})
	defer ec.abort()

	time.Sleep(50 * time.Millisecond)

	// Имитируем ReadMsg от лидера
	ec.Deliver(ReadMsg{
		Message: newTestBase(leader, "read"),
	})

	time.Sleep(100 * time.Millisecond)

	sent := pl.getSent()
	found := false
	for _, s := range sent {
		if msg, ok := s.msg.(StateMsg); ok {
			if s.to != leader {
				t.Errorf("StateMsg отправлен не лидеру: %v", s.to)
			}
			if msg.Ts != 3 {
				t.Errorf("ожидался ts=3, получен ts=%d", msg.Ts)
			}
			if msg.Val == nil || string(*msg.Val) != "old-val" {
				t.Errorf("ожидалось val=old-val, получено %v", msg.Val)
			}
			found = true
			break
		}
	}
	if !found {
		t.Fatal("follower не отправил StateMsg в ответ на ReadMsg")
	}
}

// Slave: игнорирует ReadMsg от не-лидера
func TestFollowerIgnoresReadFromNonLeader(t *testing.T) {
	ctx := context.Background()
	beb := newMockBroadcaster()
	pl := newMockLink()

	self := types.ProcessID(2)
	leader := types.ProcessID(1)
	imposter := types.ProcessID(3)

	pl.deliverer = nil

	ec := newEpochConsensus(ctx, self, beb, pl, 3)
	ec.StartEpoch(leader, 1, nil)
	defer ec.abort()

	time.Sleep(50 * time.Millisecond)

	ec.Deliver(ReadMsg{
		Message: newTestBase(imposter, "read"),
	})

	time.Sleep(100 * time.Millisecond)

	sent := pl.getSent()
	if len(sent) > 0 {
		t.Fatal("follower не должен отвечать на ReadMsg от не-лидера")
	}
}

// Slave: handleWrite обновляет current state
func TestFollowerWriteUpdatesState(t *testing.T) {
	ctx := context.Background()
	beb := newMockBroadcaster()
	pl := newMockLink()

	self := types.ProcessID(2)
	leader := types.ProcessID(1)
	epochTs := 7

	pl.deliverer = nil

	ec := newEpochConsensus(ctx, self, beb, pl, 3)
	ec.StartEpoch(leader, epochTs, nil)
	defer ec.abort()

	time.Sleep(50 * time.Millisecond)

	writeVal := ptrVal("new-value")
	ec.Deliver(WriteMsg{
		Message: newTestBase(leader, "write"),
		Val:     writeVal,
	})

	time.Sleep(100 * time.Millisecond)

	// Абортим чтобы прочитать current (после stopCh — happens-before)
	ts, val := ec.Abort()

	if ts != epochTs {
		t.Errorf("ожидался ts=%d, получен ts=%d", epochTs, ts)
	}
	if val == nil || string(*val) != "new-value" {
		t.Errorf("ожидалось val=new-value, получено %v", val)
	}
}

// Abort возвращает текущее состояние
func TestAbortReturnsCurrentState(t *testing.T) {
	ctx := context.Background()
	beb := newMockBroadcaster()
	pl := newMockLink()

	self := types.ProcessID(2)
	leader := types.ProcessID(1)

	ec := newEpochConsensus(ctx, self, beb, pl, 3)
	ec.StartEpoch(leader, 10, &state{ts: 5, val: ptrVal("saved")})

	time.Sleep(50 * time.Millisecond)

	ts, val := ec.Abort()

	if ts != 5 {
		t.Errorf("ожидался ts=5, получен ts=%d", ts)
	}
	if val == nil || /*string(*val) != "saved"*/ {
		t.Errorf("ожидалось val=saved, получено %v", val)
	}
}

// Полный happy path: 3 ноды, лидер propose → decide
func TestFullConsensusHappyPath(t *testing.T) {
	ctx := context.Background()

	leader := types.ProcessID(1)
	node2 := types.ProcessID(2)
	node3 := types.ProcessID(3)

	bebLeader := newMockBroadcaster()
	plLeader := newMockLink()

	bebNode2 := newMockBroadcaster()
	plNode2 := newMockLink()

	bebNode3 := newMockBroadcaster()
	plNode3 := newMockLink()

	ecLeader := newEpochConsensus(ctx, leader, bebLeader, plLeader, 3)
	ecNode2 := newEpochConsensus(ctx, node2, bebNode2, plNode2, 3)
	ecNode3 := newEpochConsensus(ctx, node3, bebNode3, plNode3, 3)

	// Отключаем auto-delivery в моках — будем доставлять вручную
	bebLeader.deliverer = nil
	bebNode2.deliverer = nil
	bebNode3.deliverer = nil
	plLeader.deliverer = nil
	plNode2.deliverer = nil
	plNode3.deliverer = nil

	ecLeader.StartEpoch(leader, 1, nil)
	ecNode2.StartEpoch(leader, 1, nil)
	ecNode3.StartEpoch(leader, 1, nil)

	time.Sleep(50 * time.Millisecond)

	// 1. Лидер propose
	val := types.Value("consensus-value")
	ecLeader.Propose(val)
	time.Sleep(50 * time.Millisecond)

	// 2. Доставляем ReadMsg всем (включая лидера)
	readMsgs := filterMessages[ReadMsg](bebLeader.getMessages())
	if len(readMsgs) == 0 {
		t.Fatal("лидер не отправил ReadMsg")
	}
	readMsg := readMsgs[0]

	ecLeader.Deliver(readMsg)
	ecNode2.Deliver(readMsg)
	ecNode3.Deliver(readMsg)
	time.Sleep(50 * time.Millisecond)

	// 3. Собираем StateMsg от всех нод и доставляем лидеру
	for _, s := range plLeader.getSent() {
		ecLeader.Deliver(s.msg)
	}
	for _, s := range plNode2.getSent() {
		ecLeader.Deliver(s.msg)
	}
	for _, s := range plNode3.getSent() {
		ecLeader.Deliver(s.msg)
	}
	time.Sleep(50 * time.Millisecond)

	// 4. Доставляем WriteMsg всем
	writeMsgs := filterMessages[WriteMsg](bebLeader.getMessages())
	if len(writeMsgs) == 0 {
		t.Fatal("лидер не отправил WriteMsg")
	}
	writeMsg := writeMsgs[0]

	ecLeader.Deliver(writeMsg)
	ecNode2.Deliver(writeMsg)
	ecNode3.Deliver(writeMsg)
	time.Sleep(50 * time.Millisecond)

	// 5. Собираем AcceptMsg и доставляем лидеру
	for _, s := range plLeader.getSent() {
		if _, ok := s.msg.(AcceptMsg); ok {
			ecLeader.Deliver(s.msg)
		}
	}
	for _, s := range plNode2.getSent() {
		if _, ok := s.msg.(AcceptMsg); ok {
			ecLeader.Deliver(s.msg)
		}
	}
	for _, s := range plNode3.getSent() {
		if _, ok := s.msg.(AcceptMsg); ok {
			ecLeader.Deliver(s.msg)
		}
	}
	time.Sleep(50 * time.Millisecond)

	// 6. Доставляем DecidedMsg всем
	decidedMsgs := filterMessages[DecidedMsg](bebLeader.getMessages())
	if len(decidedMsgs) == 0 {
		t.Fatal("лидер не отправил DecidedMsg")
	}
	decidedMsg := decidedMsgs[0]

	ecNode2.Deliver(decidedMsg)
	ecNode3.Deliver(decidedMsg)
	ecLeader.Deliver(decidedMsg)

	// 7. Проверяем решение
	result := waitForDecision(t, ecLeader.decided, 2*time.Second)
	if string(result) != "consensus-value" {
		t.Errorf("ожидалось consensus-value, получено %s", string(result))
	}

	result2 := waitForDecision(t, ecNode2.decided, 2*time.Second)
	if string(result2) != "consensus-value" {
		t.Errorf("node2: ожидалось consensus-value, получено %s", string(result2))
	}

	result3 := waitForDecision(t, ecNode3.decided, 2*time.Second)
	if string(result3) != "consensus-value" {
		t.Errorf("node3: ожидалось consensus-value, получено %s", string(result3))
	}
}

// Кворум: лидер решает при получении ответов от большинства, а не от всех
func TestQuorumSufficient(t *testing.T) {
	ctx := context.Background()
	beb := newMockBroadcaster()
	pl := newMockLink()

	leader := types.ProcessID(1)
	beb.deliverer = nil
	pl.deliverer = nil

	ec := newEpochConsensus(ctx, leader, beb, pl, 3)
	ec.StartEpoch(leader, 1, nil)

	time.Sleep(50 * time.Millisecond)

	val := types.IntValue(100)
	ec.Propose(val)
	time.Sleep(50 * time.Millisecond)

	// Отправляем StateMsg только от 2 из 3 (кворум = 2)
	ec.Deliver(StateMsg{
		Message: newTestBase(leader, "state"),
		Ts:      0,
		Val:     nil,
	})
	ec.Deliver(StateMsg{
		Message: newTestBase("node-2", "state"),
		Ts:      0,
		Val:     nil,
	})

	time.Sleep(100 * time.Millisecond)

	assertPhase(t, ec, StateWriting)

	ec.abort()
}

// Недостаточно голосов — остаёмся в PhaseReading
func TestInsufficientQuorumStaysReading(t *testing.T) {
	ctx := context.Background()
	beb := newMockBroadcaster()
	pl := newMockLink()

	leader := types.ProcessID("node-1")
	beb.deliverer = nil
	pl.deliverer = nil

	ec := newEpochConsensus(ctx, leader, beb, pl, 3)
	ec.StartEpoch(leader, 1, nil)

	time.Sleep(50 * time.Millisecond)

	val := types.Value("quorum-test")
	ec.Propose(val)
	time.Sleep(50 * time.Millisecond)

	// Только 1 ответ из 3 (кворум = 2)
	ec.Deliver(StateMsg{
		Message: newTestBase(leader, "state"),
		Ts:      0,
		Val:     nil,
	})

	time.Sleep(100 * time.Millisecond)

	assertPhase(t, ec, StateReading)

	ec.abort()
}

// highestState выбирает state с наибольшим timestamp
func TestHighestStateSelection(t *testing.T) {
	ctx := context.Background()
	beb := newMockBroadcaster()
	pl := newMockLink()

	leader := types.ProcessID(1)
	beb.deliverer = nil
	pl.deliverer = nil

	ec := newEpochConsensus(ctx, leader, beb, pl, 3)
	ec.StartEpoch(leader, 10, nil)

	time.Sleep(50 * time.Millisecond)

	val := types.IntValue(100)
	ec.Propose(val)
	time.Sleep(50 * time.Millisecond)

	oldVal := ptrVal("old")
	newVal := ptrVal("newer")

	ec.Deliver(StateMsg{
		Message: newTestBase(leader, "state"),
		Ts:      3,
		Val:     oldVal,
	})
	ec.Deliver(StateMsg{
		Message: newTestBase("node-2", "state"),
		Ts:      7,
		Val:     newVal,
	})

	time.Sleep(100 * time.Millisecond)

	// Проверяем что WriteMsg содержит значение с ts=7
	msgs := beb.getMessages()
	for _, m := range msgs {
		if wm, ok := m.(WriteMsg); ok {
			if wm.Val == nil || string(*wm.Val) != "newer" {
				t.Errorf("ожидалось newer (ts=7), получено %v", wm.Val)
			}
			break
		}
	}

	ec.abort()
}

// Двойной Abort не паникует
func TestDoubleAbortNoPanic(t *testing.T) {
	ctx := context.Background()
	beb := newMockBroadcaster()
	pl := newMockLink()

	self := types.ProcessID(1)
	leader := types.ProcessID(2)

	ec := newEpochConsensus(ctx, self, beb, pl, 3)
	ec.StartEpoch(leader, 1, &state{ts: 1, val: ptrVal("x")})

	time.Sleep(50 * time.Millisecond)

	// Первый abort
	ec.Abort()

	// Второй abort — не должен паниковать
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("двойной Abort вызвал панику: %v", r)
		}
	}()
	ec.Abort()
}

// ==================== Утилиты ====================

func ptrVal(s string) *types.Value {
	v := types.Value(s)
	return &v
}

func filterMessages[T types.Message](msgs []types.Message) []T {
	var result []T
	for _, m := range msgs {
		if typed, ok := m.(T); ok {
			result = append(result, typed)
		}
	}
	return result
}

// newTestBase — хелпер для создания тестовых сообщений
// Тебе нужно подставить свою реализацию в зависимости от messages.NewBase
func newTestBase(from types.ProcessID, kind string) messages.Base {
	return messages.NewBase(uuid.New(), from, kind)
}
