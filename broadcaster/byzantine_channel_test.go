package broadcaster

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"reliable/messages"
	"reliable/p2p"
	"reliable/types"
)

// ---------------------------------------------------------------------------
// mock BCB and factory collector
// ---------------------------------------------------------------------------

type mockBCB struct {
	types.Deliverer
	sender      types.ProcessID
	link        p2p.Link
	mu          sync.Mutex
	broadcasted []types.Message
	received    []types.Message
}

func newMockBCB(self, sender types.ProcessID, link p2p.Link) *mockBCB {
	return &mockBCB{
		Deliverer: types.NewUnaryDeliverer(self),
		sender:    sender,
		link:      link,
	}
}

func (b *mockBCB) Init()                         {}
func (b *mockBCB) Start()                        { b.link.AddDeliverer(b) }
func (b *mockBCB) Stop()                         {}
func (b *mockBCB) AddCorrect(types.ProcessID)    {}
func (b *mockBCB) RemoveCorrect(types.ProcessID) {}

func (b *mockBCB) Broadcast(_ context.Context, msg types.Message) {
	b.mu.Lock()
	b.broadcasted = append(b.broadcasted, msg)
	b.mu.Unlock()
}

// Deliver records inbound messages routed by the channel into this BCB.
func (b *mockBCB) Deliver(msg types.Message) {
	b.mu.Lock()
	b.received = append(b.received, msg)
	b.mu.Unlock()
}

// triggerDeliver simulates the BCB protocol completing and delivering an
// application payload to its registered deliverer (the channel's adapter).
func (b *mockBCB) triggerDeliver(msg types.Message) {
	b.Deliverer.Deliver(msg)
}

func (b *mockBCB) sentBroadcasts() []types.Message {
	b.mu.Lock()
	defer b.mu.Unlock()
	cp := make([]types.Message, len(b.broadcasted))
	copy(cp, b.broadcasted)
	return cp
}

func (b *mockBCB) receivedMessages() []types.Message {
	b.mu.Lock()
	defer b.mu.Unlock()
	cp := make([]types.Message, len(b.received))
	copy(cp, b.received)
	return cp
}

type bcbCollector struct {
	self types.ProcessID
	mu   sync.Mutex
	bcbs map[types.ProcessID][]*mockBCB
}

func newBCBCollector(self types.ProcessID) *bcbCollector {
	return &bcbCollector{
		self: self,
		bcbs: make(map[types.ProcessID][]*mockBCB),
	}
}

func (c *bcbCollector) factory() BCBFactory {
	return func(sender types.ProcessID, link p2p.Link) Broadcaster {
		bcb := newMockBCB(c.self, sender, link)
		c.mu.Lock()
		c.bcbs[sender] = append(c.bcbs[sender], bcb)
		c.mu.Unlock()
		return bcb
	}
}

func (c *bcbCollector) current(sender types.ProcessID) *mockBCB {
	c.mu.Lock()
	defer c.mu.Unlock()
	list := c.bcbs[sender]
	if len(list) == 0 {
		return nil
	}
	return list[len(list)-1]
}

func (c *bcbCollector) total(sender types.ProcessID) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.bcbs[sender])
}

// ---------------------------------------------------------------------------
// byzantineChannel (Algorithm 3.19)
// ---------------------------------------------------------------------------

func TestChannel_Init_CreatesBCBPerProcess(t *testing.T) {
	self := types.ProcessID(1)
	processes := []types.ProcessID{1, 2, 3, 4}
	link := newMockLink(self)
	registry := newTestRegistry()
	coll := newBCBCollector(self)

	ch := NewByzantineConsistentChannel(context.Background(), self, processes, link, registry, coll.factory())
	ch.Init()

	for _, p := range processes {
		assert.Equal(t, 1, coll.total(p), "one initial BCB per process")
	}
}

func TestChannel_Broadcast_DelegatesToSelfBCB(t *testing.T) {
	self := types.ProcessID(1)
	processes := []types.ProcessID{1, 2, 3, 4}
	link := newMockLink(self)
	registry := newTestRegistry()
	coll := newBCBCollector(self)

	ch := NewByzantineConsistentChannel(context.Background(), self, processes, link, registry, coll.factory())
	ch.Init()

	ch.Broadcast(context.Background(), newTestPayload(self, "hi"))

	selfBCB := coll.current(self)
	require.NotNil(t, selfBCB)
	assert.Len(t, selfBCB.sentBroadcasts(), 1)
}

func TestChannel_SecondBroadcast_QueuedUntilSelfDelivers(t *testing.T) {
	self := types.ProcessID(1)
	processes := []types.ProcessID{1, 2, 3, 4}
	link := newMockLink(self)
	registry := newTestRegistry()
	coll := newBCBCollector(self)

	ch := NewByzantineConsistentChannel(context.Background(), self, processes, link, registry, coll.factory())
	ch.Init()

	ch.Broadcast(context.Background(), newTestPayload(self, "first"))
	ch.Broadcast(context.Background(), newTestPayload(self, "second"))

	first := coll.current(self)
	assert.Len(t, first.sentBroadcasts(), 1, "second broadcast must wait for ready")
}

func TestChannel_OnSelfDeliver_InstallsNextBCBAndDrainsQueue(t *testing.T) {
	self := types.ProcessID(1)
	processes := []types.ProcessID{1, 2, 3, 4}
	link := newMockLink(self)
	registry := newTestRegistry()
	coll := newBCBCollector(self)

	ch := NewByzantineConsistentChannel(context.Background(), self, processes, link, registry, coll.factory())
	ch.Init()

	ch.Broadcast(context.Background(), newTestPayload(self, "first"))
	ch.Broadcast(context.Background(), newTestPayload(self, "second"))

	first := coll.current(self)
	first.triggerDeliver(newTestPayload(self, "first"))

	require.Eventually(t, func() bool {
		return coll.total(self) == 2
	}, time.Second, 5*time.Millisecond)

	next := coll.current(self)
	assert.Len(t, next.sentBroadcasts(), 1, "queued broadcast must fire on next BCB")
}

func TestChannel_OnBCBDelivered_OuterDeliversDomainMessage(t *testing.T) {
	self := types.ProcessID(1)
	processes := []types.ProcessID{1, 2, 3, 4}
	link := newMockLink(self)
	registry := newTestRegistry()
	coll := newBCBCollector(self)

	ch := NewByzantineConsistentChannel(context.Background(), self, processes, link, registry, coll.factory())
	capture := newCaptureDeliverer(self)
	ch.AddDeliverer(capture)
	ch.Init()

	originator := types.ProcessID(2)
	coll.current(originator).triggerDeliver(newTestPayload(originator, "hi"))

	waitForMessages(t, capture, 1)
	domain, ok := capture.messages()[0].(ByzChannelDomainMessage)
	require.True(t, ok)
	assert.Equal(t, originator, domain.From())
	assert.Equal(t, 0, domain.N)
}

func TestChannel_OnBCBDelivered_AdvancesCounterForOriginator(t *testing.T) {
	self := types.ProcessID(1)
	processes := []types.ProcessID{1, 2}
	link := newMockLink(self)
	registry := newTestRegistry()
	coll := newBCBCollector(self)

	ch := NewByzantineConsistentChannel(context.Background(), self, processes, link, registry, coll.factory())
	ch.Init()

	originator := types.ProcessID(2)
	coll.current(originator).triggerDeliver(newTestPayload(originator, "first"))

	require.Eventually(t, func() bool {
		return coll.total(originator) == 2
	}, time.Second, 5*time.Millisecond, "next BCB must be installed for originator")
}

func TestChannel_SendThroughBCBLink_TagsWithSenderAndNumber(t *testing.T) {
	self := types.ProcessID(1)
	processes := []types.ProcessID{1, 2}
	link := newMockLink(self)
	registry := newTestRegistry()
	coll := newBCBCollector(self)

	ch := NewByzantineConsistentChannel(context.Background(), self, processes, link, registry, coll.factory())
	ch.Init()

	bcb := coll.current(types.ProcessID(2))
	bcb.link.Send(types.ProcessID(2), newTestPayload(self, "ping"))

	sent := link.sentMessages()
	require.Len(t, sent, 1)
	wrapped, ok := sent[0].msg.(ByzChannelMessage)
	require.True(t, ok)
	assert.Equal(t, types.ProcessID(2), wrapped.Sender)
	assert.Equal(t, 0, wrapped.Number)
}

func TestChannel_IncomingWrapped_RoutedToCurrentBCB(t *testing.T) {
	self := types.ProcessID(1)
	processes := []types.ProcessID{1, 2}
	link := newMockLink(self)
	registry := newTestRegistry()
	coll := newBCBCollector(self)

	ch := NewByzantineConsistentChannel(context.Background(), self, processes, link, registry, coll.factory())
	ch.Init()

	originator := types.ProcessID(2)
	innerPayload := newTestPayload(originator, "hi")
	innerRaw, err := registry.Marshal(innerPayload)
	require.NoError(t, err)

	wrapped := ByzChannelMessage{
		BaseMsg: messages.NewBase(uuid.New(), originator, ByzChannelMessageName),
		Inner:   messages.NewRaw(innerRaw, innerPayload.Type()),
		Sender:  originator,
		Number:  0,
	}
	link.deliverIncoming(wrapped)

	bcb := coll.current(originator)
	require.Eventually(t, func() bool {
		return len(bcb.receivedMessages()) > 0
	}, time.Second, 5*time.Millisecond)
}

func TestChannel_IncomingWrapped_WrongNumberDropped(t *testing.T) {
	self := types.ProcessID(1)
	processes := []types.ProcessID{1, 2}
	link := newMockLink(self)
	registry := newTestRegistry()
	coll := newBCBCollector(self)

	ch := NewByzantineConsistentChannel(context.Background(), self, processes, link, registry, coll.factory())
	ch.Init()

	originator := types.ProcessID(2)
	innerPayload := newTestPayload(originator, "hi")
	innerRaw, err := registry.Marshal(innerPayload)
	require.NoError(t, err)

	wrapped := ByzChannelMessage{
		BaseMsg: messages.NewBase(uuid.New(), originator, ByzChannelMessageName),
		Inner:   messages.NewRaw(innerRaw, innerPayload.Type()),
		Sender:  originator,
		Number:  5, // current n[originator] is 0
	}
	link.deliverIncoming(wrapped)

	assert.Empty(t, coll.current(originator).receivedMessages())
}
