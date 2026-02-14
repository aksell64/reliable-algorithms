package mocks

import (
	"reliable/types"
	"sync"

	"github.com/google/uuid"
)

// MockLink реализует p2p.Link без сети.
type MockLink struct {
	mu         sync.Mutex
	deliverers []types.Deliverer
	sent       []LinkSentMsg
	sendCh     chan LinkSentMsg
	pid        types.ProcessID
	id         uuid.UUID
}

type LinkSentMsg struct {
	To  types.ProcessID
	Msg types.Message
}

func NewMockLink(pid types.ProcessID) *MockLink {
	return &MockLink{
		sent:   make([]LinkSentMsg, 0),
		sendCh: make(chan LinkSentMsg, 100),
		id:     uuid.New(),
		pid:    pid,
	}
}

func (l *MockLink) Init()                      {}
func (l *MockLink) Start()                     {}
func (l *MockLink) Stop()                      {}
func (l *MockLink) ID() uuid.UUID              { return l.id }
func (l *MockLink) Instance() string           { return "mock_pl" }
func (l *MockLink) ProcessID() types.ProcessID { return l.pid }

func (l *MockLink) Deliver(msg types.Message) {}

func (l *MockLink) Send(to types.ProcessID, msg types.Message) {
	l.mu.Lock()
	sm := LinkSentMsg{To: to, Msg: msg}
	l.sent = append(l.sent, sm)
	l.mu.Unlock()
	select {
	case l.sendCh <- sm:
	default:
	}
}

func (l *MockLink) AddDeliverer(d types.Deliverer, opts ...types.DelivererOption) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.deliverers = append(l.deliverers, d)
}

func (l *MockLink) RemoveDeliverer(d types.Deliverer) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for i, dd := range l.deliverers {
		if dd == d {
			l.deliverers = append(l.deliverers[:i], l.deliverers[i+1:]...)
			break
		}
	}
}

func (l *MockLink) GetSent() []LinkSentMsg {
	l.mu.Lock()
	defer l.mu.Unlock()
	cp := make([]LinkSentMsg, len(l.sent))
	copy(cp, l.sent)
	return cp
}

func (l *MockLink) SentCh() chan LinkSentMsg {
	return l.sendCh
}
