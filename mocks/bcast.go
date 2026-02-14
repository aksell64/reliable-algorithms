package mocks

import (
	"context"
	"reliable/types"
	"sync"

	"github.com/google/uuid"
)

// MockBroadcaster реализует broadcaster.Broadcaster без сети.
// Сохраняет все broadcast-сообщения и позволяет доставлять их вручную.
type MockBroadcaster struct {
	mu          sync.Mutex
	id          uuid.UUID
	deliverers  []types.Deliverer
	broadcasted []types.Message
	broadcastCh chan types.Message
	self        types.ProcessID
}

func NewMockBroadcaster(procID types.ProcessID) *MockBroadcaster {
	return &MockBroadcaster{
		broadcasted: make([]types.Message, 0),
		broadcastCh: make(chan types.Message, 100),
		self:        procID,
		id:          uuid.New(),
	}
}

func (b *MockBroadcaster) Init()                            {}
func (b *MockBroadcaster) Start()                           {}
func (b *MockBroadcaster) Stop()                            {}
func (b *MockBroadcaster) ID() uuid.UUID                    { return b.id }
func (b *MockBroadcaster) Instance() string                 { return "mock_beb" }
func (b *MockBroadcaster) ProcessID() types.ProcessID       { return b.self }
func (b *MockBroadcaster) AddCorrect(pid types.ProcessID)   {}
func (b *MockBroadcaster) RemoveCorrect(id types.ProcessID) {}

func (b *MockBroadcaster) Deliver(msg types.Message) {}

func (b *MockBroadcaster) Broadcast(ctx context.Context, msg types.Message) {
	b.mu.Lock()
	b.broadcasted = append(b.broadcasted, msg)
	b.mu.Unlock()
	select {
	case b.broadcastCh <- msg:
	default:
	}
}

func (b *MockBroadcaster) AddDeliverer(d types.Deliverer, opts ...types.DelivererOption) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.deliverers = append(b.deliverers, d)
}

func (b *MockBroadcaster) RemoveDeliverer(d types.Deliverer) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for i, dd := range b.deliverers {
		if dd == d {
			b.deliverers = append(b.deliverers[:i], b.deliverers[i+1:]...)
			break
		}
	}
}

func (b *MockBroadcaster) GetBroadcasted() []types.Message {
	b.mu.Lock()
	defer b.mu.Unlock()
	cp := make([]types.Message, len(b.broadcasted))
	copy(cp, b.broadcasted)
	return cp
}

func (b *MockBroadcaster) BcastCh() chan types.Message {
	return b.broadcastCh
}
