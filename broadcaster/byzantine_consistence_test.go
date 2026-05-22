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
	"reliable/types"
	"reliable/utils/codec"
)

// ---------------------------------------------------------------------------
// shared mocks & helpers
// ---------------------------------------------------------------------------

type mockLink struct {
	types.UnimplementedDeliverer
	mu         sync.Mutex
	self       types.ProcessID
	sent       []sentRecord
	deliverers []types.Deliverer
}

type sentRecord struct {
	to  types.ProcessID
	msg types.Message
}

func newMockLink(self types.ProcessID) *mockLink {
	return &mockLink{
		UnimplementedDeliverer: types.NewUnimplementedDeliverer(self),
		self:                   self,
	}
}

func (l *mockLink) Send(to types.ProcessID, msg types.Message) {
	l.mu.Lock()
	l.sent = append(l.sent, sentRecord{to, msg})
	l.mu.Unlock()
}

func (l *mockLink) AddDeliverer(d types.Deliverer, _ ...types.DelivererOption) {
	l.mu.Lock()
	l.deliverers = append(l.deliverers, d)
	l.mu.Unlock()
}

func (l *mockLink) RemoveDeliverer(d types.Deliverer) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for i, dd := range l.deliverers {
		if dd.ID() == d.ID() {
			l.deliverers = append(l.deliverers[:i], l.deliverers[i+1:]...)
			return
		}
	}
}

func (l *mockLink) Init()  {}
func (l *mockLink) Start() {}
func (l *mockLink) Stop()  {}

// deliverIncoming simulates an inbound network message by fanning it out to
// every registered deliverer.
func (l *mockLink) deliverIncoming(msg types.Message) {
	l.mu.Lock()
	ds := make([]types.Deliverer, len(l.deliverers))
	copy(ds, l.deliverers)
	l.mu.Unlock()
	for _, d := range ds {
		d.Deliver(msg)
	}
}

func (l *mockLink) sentMessages() []sentRecord {
	l.mu.Lock()
	defer l.mu.Unlock()
	cp := make([]sentRecord, len(l.sent))
	copy(cp, l.sent)
	return cp
}

// captureDeliverer records every Deliver call.
type captureDeliverer struct {
	types.UnimplementedDeliverer
	mu  sync.Mutex
	got []types.Message
}

func newCaptureDeliverer(self types.ProcessID) *captureDeliverer {
	return &captureDeliverer{
		UnimplementedDeliverer: types.NewUnimplementedDeliverer(self),
	}
}

func (c *captureDeliverer) Deliver(msg types.Message) {
	c.mu.Lock()
	c.got = append(c.got, msg)
	c.mu.Unlock()
}

func (c *captureDeliverer) messages() []types.Message {
	c.mu.Lock()
	defer c.mu.Unlock()
	cp := make([]types.Message, len(c.got))
	copy(cp, c.got)
	return cp
}

// stubIdentify accepts every signature.
type stubIdentify struct{}

func (stubIdentify) Sign(raw []byte) ([]byte, error)              { return raw, nil }
func (stubIdentify) Verify(types.ProcessID, []byte, []byte) error { return nil }

const testPayloadName = "test_payload"

type testPayload struct {
	messages.BaseMsg
	Data string
}

func (m testPayload) Type() string { return testPayloadName }

func newTestPayload(from types.ProcessID, data string) testPayload {
	return testPayload{
		BaseMsg: messages.NewBase(uuid.New(), from, testPayloadName),
		Data:    data,
	}
}

func newTestRegistry() *codec.Registry {
	r := codec.New()
	codec.Register[testPayload](r, testPayloadName)
	return r
}

func marshalInner(t *testing.T, r *codec.Registry, p testPayload) messages.RawMsg {
	t.Helper()
	raw, err := r.Marshal(p)
	require.NoError(t, err)
	return messages.NewRaw(raw, p.Type())
}

func waitForMessages(t *testing.T, c *captureDeliverer, n int) {
	t.Helper()
	assert.Eventually(t, func() bool {
		return len(c.messages()) >= n
	}, time.Second, 5*time.Millisecond, "expected %d delivered messages", n)
}

// ---------------------------------------------------------------------------
// authenticatedEchoBroadcaster (Algorithm 3.10)
// ---------------------------------------------------------------------------

func TestAuthenticatedEcho_Broadcast_SendsBcastToAllProcesses(t *testing.T) {
	processes := []types.ProcessID{1, 2, 3, 4}
	self, sender := types.ProcessID(1), types.ProcessID(1)
	registry := newTestRegistry()
	link := newMockLink(self)

	bcb := NewAuthenticatedEchoBroadcaster(self, sender, link, processes, 1, registry)
	bcb.Init()
	bcb.Start()

	bcb.Broadcast(context.Background(), newTestPayload(self, "hi"))

	sent := link.sentMessages()
	require.Len(t, sent, len(processes))
	for _, rec := range sent {
		_, ok := rec.msg.(ByzConsistenceMessage)
		assert.True(t, ok)
	}
}

func TestAuthenticatedEcho_OnSendFromSender_BroadcastsEcho(t *testing.T) {
	processes := []types.ProcessID{1, 2, 3, 4}
	self, sender := types.ProcessID(2), types.ProcessID(1)
	registry := newTestRegistry()
	link := newMockLink(self)

	bcb := NewAuthenticatedEchoBroadcaster(self, sender, link, processes, 1, registry)
	bcb.Init()
	bcb.Start()

	sendMsg := ByzConsistenceMessage{
		BaseMsg: messages.NewBase(uuid.New(), sender, ByzConsistenceMessageName),
		RawMsg:  marshalInner(t, registry, newTestPayload(sender, "hi")),
	}
	link.deliverIncoming(sendMsg)

	sent := link.sentMessages()
	require.Len(t, sent, len(processes))
	for _, rec := range sent {
		_, ok := rec.msg.(ByzConsistenceEchoMessage)
		assert.True(t, ok)
	}
}

func TestAuthenticatedEcho_OnSendFromNonSender_NoEcho(t *testing.T) {
	processes := []types.ProcessID{1, 2, 3, 4}
	self, sender := types.ProcessID(2), types.ProcessID(1)
	registry := newTestRegistry()
	link := newMockLink(self)

	bcb := NewAuthenticatedEchoBroadcaster(self, sender, link, processes, 1, registry)
	bcb.Init()
	bcb.Start()

	impostor := types.ProcessID(3)
	sendMsg := ByzConsistenceMessage{
		BaseMsg: messages.NewBase(uuid.New(), impostor, ByzConsistenceMessageName),
		RawMsg:  marshalInner(t, registry, newTestPayload(impostor, "hi")),
	}
	link.deliverIncoming(sendMsg)

	assert.Empty(t, link.sentMessages())
}

func TestAuthenticatedEcho_QuorumOfEchos_DeliversInner(t *testing.T) {
	processes := []types.ProcessID{1, 2, 3, 4}
	self, sender := types.ProcessID(1), types.ProcessID(1)
	registry := newTestRegistry()
	link := newMockLink(self)

	bcb := NewAuthenticatedEchoBroadcaster(self, sender, link, processes, 1, registry)
	bcb.Init()
	bcb.Start()

	capture := newCaptureDeliverer(self)
	bcb.AddDeliverer(capture)

	inner := marshalInner(t, registry, newTestPayload(sender, "hi"))
	for _, p := range processes {
		echo := ByzConsistenceEchoMessage{
			BaseMsg: messages.NewBase(uuid.New(), p, ByzConsistenceEchoMessageName),
			Inner:   inner,
		}
		link.deliverIncoming(echo)
	}

	waitForMessages(t, capture, 1)
	delivered, ok := capture.messages()[0].(*testPayload)
	require.True(t, ok)
	assert.Equal(t, "hi", delivered.Data)
}

// ---------------------------------------------------------------------------
// signedEchoBroadcast (Algorithm 3.17)
// ---------------------------------------------------------------------------

func TestSignedEcho_Broadcast_FromNonSenderIsNoop(t *testing.T) {
	processes := []types.ProcessID{1, 2, 3, 4}
	sender := types.ProcessID(1)
	registry := newTestRegistry()
	link := newMockLink(2)

	bcb := NewSignedEchoBroadcaster(2, sender, processes, 1, stubIdentify{}, link, registry)
	bcb.Init()
	bcb.Start()
	bcb.Broadcast(context.Background(), newTestPayload(2, "hi"))

	assert.Empty(t, link.sentMessages())
}

func TestSignedEcho_Broadcast_FromSender_SendsToAll(t *testing.T) {
	processes := []types.ProcessID{1, 2, 3, 4}
	sender := types.ProcessID(1)
	registry := newTestRegistry()
	link := newMockLink(sender)

	bcb := NewSignedEchoBroadcaster(sender, sender, processes, 1, stubIdentify{}, link, registry)
	bcb.Init()
	bcb.Start()
	bcb.Broadcast(context.Background(), newTestPayload(sender, "hi"))

	sent := link.sentMessages()
	require.Len(t, sent, len(processes))
	for _, rec := range sent {
		_, ok := rec.msg.(ByzConsistenceMessage)
		assert.True(t, ok)
	}
}

func TestSignedEcho_OnSendFromSender_EchoesOnlyToSender(t *testing.T) {
	processes := []types.ProcessID{1, 2, 3, 4}
	self, sender := types.ProcessID(2), types.ProcessID(1)
	registry := newTestRegistry()
	link := newMockLink(self)

	bcb := NewSignedEchoBroadcaster(self, sender, processes, 1, stubIdentify{}, link, registry)
	bcb.Init()
	bcb.Start()

	sendMsg := ByzConsistenceMessage{
		BaseMsg: messages.NewBase(uuid.New(), sender, ByzConsistenceMessageName),
		RawMsg:  marshalInner(t, registry, newTestPayload(sender, "hi")),
	}
	link.deliverIncoming(sendMsg)

	sent := link.sentMessages()
	require.Len(t, sent, 1)
	assert.Equal(t, sender, sent[0].to)
	_, ok := sent[0].msg.(ByzConsistenceSignedEchoMessage)
	assert.True(t, ok)
}

func TestSignedEcho_QuorumOfEchos_BroadcastsFinal(t *testing.T) {
	processes := []types.ProcessID{1, 2, 3, 4}
	self, sender := types.ProcessID(1), types.ProcessID(1) // we are s
	registry := newTestRegistry()
	link := newMockLink(self)

	bcb := NewSignedEchoBroadcaster(self, sender, processes, 1, stubIdentify{}, link, registry)
	bcb.Init()
	bcb.Start()

	inner := marshalInner(t, registry, newTestPayload(sender, "hi"))
	for _, p := range processes {
		echo := ByzConsistenceSignedEchoMessage{
			BaseMsg: messages.NewBase(uuid.New(), p, ByzConsistenceSignedEchoMessageName),
			Inner:   inner,
			Sign:    []byte("sig"),
		}
		link.deliverIncoming(echo)
	}

	foundFinal := false
	for _, rec := range link.sentMessages() {
		if _, ok := rec.msg.(ByzConsistenceFinalMessage); ok {
			foundFinal = true
			break
		}
	}
	assert.True(t, foundFinal, "should broadcast FINAL after quorum of ECHOs")
}

func TestSignedEcho_ValidFinal_DeliversInner(t *testing.T) {
	processes := []types.ProcessID{1, 2, 3, 4}
	self, sender := types.ProcessID(2), types.ProcessID(1)
	registry := newTestRegistry()
	link := newMockLink(self)

	bcb := NewSignedEchoBroadcaster(self, sender, processes, 1, stubIdentify{}, link, registry)
	bcb.Init()
	bcb.Start()

	capture := newCaptureDeliverer(self)
	bcb.AddDeliverer(capture)

	inner := marshalInner(t, registry, newTestPayload(sender, "hi"))
	finalMsg := ByzConsistenceFinalMessage{
		BaseMsg: messages.NewBase(uuid.New(), sender, ByzConsistenceFinalMessageName),
		Inner:   inner,
		Signs: []ByzConsistenceFinalMessageSign{
			{ProcessID: 1, Sign: []byte("s1")},
			{ProcessID: 2, Sign: []byte("s2")},
			{ProcessID: 3, Sign: []byte("s3")},
			{ProcessID: 4, Sign: []byte("s4")},
		},
	}
	link.deliverIncoming(finalMsg)

	waitForMessages(t, capture, 1)
	delivered, ok := capture.messages()[0].(*testPayload)
	require.True(t, ok)
	assert.Equal(t, "hi", delivered.Data)
}
