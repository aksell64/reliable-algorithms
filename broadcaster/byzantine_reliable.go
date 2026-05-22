package broadcaster

import (
	"context"
	"reliable/messages"
	"reliable/p2p"
	"reliable/types"
	"reliable/utils/codec"
	"sync"

	"github.com/google/uuid"
)

type authenticatedDoubleEchoBroadcaster struct {
	types.Deliverer
	self      types.ProcessID
	sender    types.ProcessID
	processes []types.ProcessID
	faults    int
	al        p2p.Link
	sentEcho  bool
	sentReady bool
	delivered bool
	echos     map[types.ProcessID]ByzReliableEchoMessage
	readys    map[types.ProcessID]ByzReliableReadyMessage
	registry  *codec.Registry
	mu        sync.RWMutex
}

func NewAuthenticatedDoubleEchoBroadcaster(
	self types.ProcessID,
	sender types.ProcessID,
	processes []types.ProcessID,
	faults int,
	al p2p.Link,
	registry *codec.Registry,
) Broadcaster {
	b := new(authenticatedDoubleEchoBroadcaster)
	b.self = self
	b.sender = sender
	b.processes = processes
	b.faults = faults
	b.al = al
	b.registry = registry
	b.Deliverer = types.NewUnaryDeliverer(self)
	return b
}

func (b *authenticatedDoubleEchoBroadcaster) AddCorrect(pid types.ProcessID) {
	b.mu.Lock()
	defer b.mu.Unlock()
	addToProcessesSlice(&b.processes, pid)
}

func (b *authenticatedDoubleEchoBroadcaster) RemoveCorrect(pid types.ProcessID) {
	b.mu.Lock()
	defer b.mu.Unlock()
	removeFromProcessesSlice(&b.processes, pid)
}

func (b *authenticatedDoubleEchoBroadcaster) Init() {
	codec.Register[ByzReliableMessage](b.registry, ByzReliableMessageName)
	codec.Register[ByzReliableEchoMessage](b.registry, ByzReliableEchoMessageName)
	codec.Register[ByzReliableReadyMessage](b.registry, ByzReliableReadyMessageName)
	b.echos = make(map[types.ProcessID]ByzReliableEchoMessage)
	b.readys = make(map[types.ProcessID]ByzReliableReadyMessage)
}

func (b *authenticatedDoubleEchoBroadcaster) Start() {
	b.al.AddDeliverer(b)
}

func (b *authenticatedDoubleEchoBroadcaster) Stop() {
	b.al.RemoveDeliverer(b)
}

func (b *authenticatedDoubleEchoBroadcaster) Broadcast(ctx context.Context, msg types.Message) {
	if b.self != b.sender {
		return
	}

	raw, err := b.registry.Marshal(msg)
	if err != nil {
		return
	}
	bmsg := ByzReliableMessage{
		BaseMsg: messages.NewBase(uuid.New(), b.self, ByzReliableMessageName),
		RawMsg:  messages.NewRaw(raw, msg.Type()),
	}
	b.alBroadcast(bmsg)
}

func (b *authenticatedDoubleEchoBroadcaster) Deliver(msg types.Message) {
	switch m := msg.(type) {
	case ByzReliableMessage:
		b.handleSendMessage(m)
	case ByzReliableEchoMessage:
		b.handleEchoMessage(m)
	case ByzReliableReadyMessage:
		b.handleReadyMessage(m)
	}
}

func (b *authenticatedDoubleEchoBroadcaster) handleSendMessage(msg ByzReliableMessage) {
	if msg.From() != b.sender || b.sentEcho {
		return
	}

	b.sentEcho = true
	emsg := ByzReliableEchoMessage{
		BaseMsg: messages.NewBase(uuid.New(), b.self, ByzReliableEchoMessageName),
		Inner:   msg.RawMsg,
	}

	b.alBroadcast(emsg)
}

func (b *authenticatedDoubleEchoBroadcaster) handleEchoMessage(msg ByzReliableEchoMessage) {
	_, exists := b.echos[msg.From()]
	if exists {
		return
	}
	b.echos[msg.From()] = msg

	ready, quorumMsg := b.checkReadyStage()
	if !ready {
		return
	}
	b.sentReady = true

	readyMsg := ByzReliableReadyMessage{
		BaseMsg: messages.NewBase(uuid.New(), b.self, ByzReliableReadyMessageName),
		Inner:   quorumMsg.Inner,
	}
	b.alBroadcast(readyMsg)
}

func (b *authenticatedDoubleEchoBroadcaster) handleReadyMessage(msg ByzReliableReadyMessage) {
	_, exists := b.readys[msg.From()]
	if exists {
		return
	}
	b.readys[msg.From()] = msg

	canEnhance, readyMsg := b.checkEnhanceReady()
	if canEnhance {
		b.sentReady = true
		b.alBroadcast(readyMsg)
	}

	canDeliver, readyMsg := b.checkDeliver()
	if canDeliver {
		b.delivered = true
		msg, err := messages.UnmarshalRawWithRegistry(readyMsg.Inner, b.registry)
		if err != nil {
			return
		}
		b.Deliverer.Deliver(msg)
	}
}

func (b *authenticatedDoubleEchoBroadcaster) checkDeliver() (bool, ByzReliableReadyMessage) {
	if b.delivered {
		return false, ByzReliableReadyMessage{}
	}

	for pid, msg := range b.readys {
		count := 0
		for otherPid, otherMsg := range b.readys {
			if pid == otherPid {
				continue
			}
			if msg.Inner.Equal(otherMsg.Inner) {
				count++
				if count > 2*b.faults {
					return true, msg
				}
			}
		}
	}
	return false, ByzReliableReadyMessage{}
}

func (b *authenticatedDoubleEchoBroadcaster) checkEnhanceReady() (bool, ByzReliableReadyMessage) {
	if b.sentReady {
		return false, ByzReliableReadyMessage{}
	}

	for pid, msg := range b.readys {
		count := 0
		for otherPid, otherMsg := range b.readys {
			if pid == otherPid {
				continue
			}
			if msg.Inner.Equal(otherMsg.Inner) {
				count++
				if count > b.faults {
					return true, msg
				}
			}
		}
	}
	return false, ByzReliableReadyMessage{}
}

func (b *authenticatedDoubleEchoBroadcaster) checkReadyStage() (bool, ByzReliableEchoMessage) {
	if b.sentReady {
		return false, ByzReliableEchoMessage{}
	}

	for pid, msg := range b.echos {
		count := 0
		for otherPid, otherMsg := range b.echos {
			if pid == otherPid {
				continue
			}
			if msg.Inner.Equal(otherMsg.Inner) {
				count++
				if count > b.byzantineQuorum() {
					return true, msg
				}
			}
		}
	}
	return false, ByzReliableEchoMessage{}
}

func (b *authenticatedDoubleEchoBroadcaster) byzantineQuorum() int {
	return (len(b.processes) + b.faults) / 2
}

func (b *authenticatedDoubleEchoBroadcaster) alBroadcast(msg types.Message) {
	processes := make([]types.ProcessID, 0, len(b.processes))
	b.mu.RLock()
	for _, p := range b.processes {
		processes = append(processes, p)
	}
	b.mu.RUnlock()

	for _, p := range processes {
		b.al.Send(p, msg)
	}
}
