package broadcaster

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"reliable/messages"
	"reliable/types"
)

// ---------------------------------------------------------------------------
// authenticatedDoubleEchoBroadcaster (Algorithm 3.18)
// ---------------------------------------------------------------------------

func TestDoubleEcho_Broadcast_FromNonSenderIsNoop(t *testing.T) {
	processes := []types.ProcessID{1, 2, 3, 4}
	registry := newTestRegistry()
	link := newMockLink(2)

	bcb := NewAuthenticatedDoubleEchoBroadcaster(2, 1, processes, 1, link, registry)
	bcb.Init()
	bcb.Start()
	bcb.Broadcast(context.Background(), newTestPayload(2, "hi"))

	assert.Empty(t, link.sentMessages())
}

func TestDoubleEcho_Broadcast_FromSender_SendsToAll(t *testing.T) {
	processes := []types.ProcessID{1, 2, 3, 4}
	sender := types.ProcessID(1)
	registry := newTestRegistry()
	link := newMockLink(sender)

	bcb := NewAuthenticatedDoubleEchoBroadcaster(sender, sender, processes, 1, link, registry)
	bcb.Init()
	bcb.Start()
	bcb.Broadcast(context.Background(), newTestPayload(sender, "hi"))

	sent := link.sentMessages()
	require.Len(t, sent, len(processes))
	for _, rec := range sent {
		_, ok := rec.msg.(ByzReliableMessage)
		assert.True(t, ok)
	}
}

func TestDoubleEcho_OnSendFromSender_BroadcastsEcho(t *testing.T) {
	processes := []types.ProcessID{1, 2, 3, 4}
	self, sender := types.ProcessID(2), types.ProcessID(1)
	registry := newTestRegistry()
	link := newMockLink(self)

	bcb := NewAuthenticatedDoubleEchoBroadcaster(self, sender, processes, 1, link, registry)
	bcb.Init()
	bcb.Start()

	sendMsg := ByzReliableMessage{
		BaseMsg: messages.NewBase(uuid.New(), sender, ByzReliableMessageName),
		RawMsg:  marshalInner(t, registry, newTestPayload(sender, "hi")),
	}
	link.deliverIncoming(sendMsg)

	sent := link.sentMessages()
	require.Len(t, sent, len(processes))
	for _, rec := range sent {
		_, ok := rec.msg.(ByzReliableEchoMessage)
		assert.True(t, ok)
	}
}

func TestDoubleEcho_QuorumOfEchos_BroadcastsReady(t *testing.T) {
	processes := []types.ProcessID{1, 2, 3, 4}
	self, sender := types.ProcessID(2), types.ProcessID(1)
	registry := newTestRegistry()
	link := newMockLink(self)

	bcb := NewAuthenticatedDoubleEchoBroadcaster(self, sender, processes, 1, link, registry)
	bcb.Init()
	bcb.Start()

	inner := marshalInner(t, registry, newTestPayload(sender, "hi"))
	for _, p := range processes {
		echo := ByzReliableEchoMessage{
			BaseMsg: messages.NewBase(uuid.New(), p, ByzReliableEchoMessageName),
			Inner:   inner,
		}
		link.deliverIncoming(echo)
	}

	foundReady := false
	for _, rec := range link.sentMessages() {
		if _, ok := rec.msg.(ByzReliableReadyMessage); ok {
			foundReady = true
			break
		}
	}
	assert.True(t, foundReady, "should broadcast READY after quorum of ECHOs")
}

func TestDoubleEcho_QuorumOfReadys_DeliversInner(t *testing.T) {
	processes := []types.ProcessID{1, 2, 3, 4}
	self, sender := types.ProcessID(2), types.ProcessID(1)
	registry := newTestRegistry()
	link := newMockLink(self)

	bcb := NewAuthenticatedDoubleEchoBroadcaster(self, sender, processes, 1, link, registry)
	bcb.Init()
	bcb.Start()

	capture := newCaptureDeliverer(self)
	bcb.AddDeliverer(capture)

	inner := marshalInner(t, registry, newTestPayload(sender, "hi"))
	for _, p := range processes {
		ready := ByzReliableReadyMessage{
			BaseMsg: messages.NewBase(uuid.New(), p, ByzReliableReadyMessageName),
			Inner:   inner,
		}
		link.deliverIncoming(ready)
	}

	msgs := capture.messages()
	require.NotEmpty(t, msgs, "must deliver inner message after quorum of READYs")
	delivered, ok := msgs[0].(*testPayload)
	require.True(t, ok)
	assert.Equal(t, "hi", delivered.Data)
}
