package broadcaster

import (
	"context"
	"encoding/json"
	"reliable/messages"
	"reliable/p2p"
	"reliable/types"
	"reliable/utils/codec"
	"sync"

	"github.com/google/uuid"
)

type authenticatedEchoBroadcaster struct {
	types.Deliverer
	self      types.ProcessID
	sender    types.ProcessID
	al        p2p.Link
	sentEcho  bool
	delivered bool
	echos     map[types.ProcessID]ByzConsistenceEchoMessage
	mu        sync.RWMutex
	processes []types.ProcessID
	faults    int
	registry  *codec.Registry
}

func NewAuthenticatedEchoBroadcaster(
	self types.ProcessID,
	sender types.ProcessID,
	al p2p.Link,
	processes []types.ProcessID,
	faults int,
	registry *codec.Registry,
) Broadcaster {
	b := new(authenticatedEchoBroadcaster)
	b.self = self
	b.sender = sender
	b.al = al
	b.processes = processes
	b.faults = faults
	b.registry = registry
	b.Deliverer = types.NewUnaryDeliverer(self)
	return b
}

func (b *authenticatedEchoBroadcaster) Broadcast(ctx context.Context, msg types.Message) {
	raw, err := b.registry.Marshal(msg)
	if err != nil {
		return
	}

	bcastMsg := ByzConsistenceMessage{
		BaseMsg: messages.NewBase(uuid.New(), b.self, ByzConsistenceMessageName),
		RawMsg:  messages.NewRaw(raw, msg.Type()),
	}

	b.alBroadcast(bcastMsg)
}

func (b *authenticatedEchoBroadcaster) Deliver(msg types.Message) {
	switch m := msg.(type) {
	case ByzConsistenceMessage:
		if b.sentEcho || m.From() != b.sender {
			return
		}

		b.sentEcho = true

		echoMsg := ByzConsistenceEchoMessage{
			BaseMsg: messages.NewBase(uuid.New(), b.self, ByzConsistenceEchoMessageName),
			Inner:   m.RawMsg,
		}
		b.alBroadcast(echoMsg)

	case ByzConsistenceEchoMessage:
		b.mu.Lock()
		defer b.mu.Unlock()
		if _, exists := b.echos[m.From()]; !exists {
			b.echos[m.From()] = m
		}
		b.maybeDeliverInner()
	}

}

func (b *authenticatedEchoBroadcaster) AddCorrect(pid types.ProcessID) {
	addToProcessesSlice(&b.processes, pid)
}

func (b *authenticatedEchoBroadcaster) RemoveCorrect(id types.ProcessID) {
	removeFromProcessesSlice(&b.processes, id)
}

func (b *authenticatedEchoBroadcaster) Init() {
	codec.Register[ByzConsistenceMessage](b.registry, ByzConsistenceMessageName)
	codec.Register[ByzConsistenceEchoMessage](b.registry, ByzConsistenceEchoMessageName)
	b.echos = make(map[types.ProcessID]ByzConsistenceEchoMessage)
}

func (b *authenticatedEchoBroadcaster) Start() {
	b.al.AddDeliverer(b)
}

func (b *authenticatedEchoBroadcaster) Stop() {
	b.al.RemoveDeliverer(b)
}

func (b *authenticatedEchoBroadcaster) alBroadcast(msg types.Message) {
	for _, p := range b.processes {
		b.al.Send(p, msg)
	}
}

func (b *authenticatedEchoBroadcaster) maybeDeliverInner() {
	if b.delivered {
		return
	}

	for from, msg := range b.echos {
		processesCount := 0

		for fromOther, other := range b.echos {
			if from == fromOther {
				continue
			}
			if !msg.BaseMsg.Equal(other.BaseMsg) {
				continue
			}
			if !msg.Inner.Equal(other.Inner) {
				continue
			}

			processesCount++

			if processesCount > (len(b.processes)+b.faults)/2 {
				b.delivered = true
				innerMsgTyp, err := b.registry.Unmarshal(msg.Inner.Raw, msg.Inner.RawType)
				if err != nil {
					return
				}

				innerMsg, ok := innerMsgTyp.(types.Message)
				if !ok {
					return
				}

				go b.Deliverer.Deliver(innerMsg)
				return
			}
		}
	}
}

type signedEchoBroadcast struct {
	types.Deliverer
	self      types.ProcessID
	processes []types.ProcessID
	faults    int
	sender    types.ProcessID
	sentEcho  bool
	sentFinal bool
	delivered bool
	echos     map[types.ProcessID]ByzConsistenceSignedEchoMessage
	identify  types.Identify
	al        p2p.Link
	registry  *codec.Registry
}

func NewSignedEchoBroadcaster(
	self types.ProcessID,
	processes []types.ProcessID,
	faults int,
	identify types.Identify,
	al p2p.Link,
	registry *codec.Registry,
) Broadcaster {
	b := new(signedEchoBroadcast)
	b.self = self
	b.processes = processes
	b.faults = faults
	b.identify = identify
	b.al = al
	b.registry = registry
	b.Deliverer = types.NewUnaryDeliverer(self)
	return b
}

func (b *signedEchoBroadcast) Broadcast(ctx context.Context, msg types.Message) {
	raw, err := b.registry.Marshal(msg)
	if err != nil {
		return
	}

	bcastMsg := ByzConsistenceMessage{
		BaseMsg: messages.NewBase(uuid.New(), b.self, ByzConsistenceMessageName),
		RawMsg:  messages.NewRaw(raw, msg.Type()),
	}

	b.alBroadcast(bcastMsg)
}

func (b *signedEchoBroadcast) Deliver(msg types.Message) {
	switch m := msg.(type) {
	case ByzConsistenceMessage:
		b.handleBroadcastedMsg(m)
	case ByzConsistenceSignedEchoMessage:
		b.handleEchoMsg(m)
	case ByzConsistenceFinalMessage:
		b.handleFinal(m)
	}
}

func (b *signedEchoBroadcast) Init() {
	codec.Register[ByzConsistenceMessage](b.registry, ByzConsistenceMessageName)
	codec.Register[ByzConsistenceSignedEchoMessage](b.registry, ByzConsistenceSignedEchoMessageName)
	codec.Register[ByzConsistenceFinalMessage](b.registry, ByzConsistenceFinalMessageName)
	b.echos = make(map[types.ProcessID]ByzConsistenceSignedEchoMessage)
}

func (b *signedEchoBroadcast) Start() {
	b.al.AddDeliverer(b)
}

func (b *signedEchoBroadcast) Stop() {
	b.al.RemoveDeliverer(b)
}

func (b *signedEchoBroadcast) AddCorrect(p types.ProcessID) {
	addToProcessesSlice(&b.processes, p)
}

func (b *signedEchoBroadcast) RemoveCorrect(p types.ProcessID) {
	removeFromProcessesSlice(&b.processes, p)
}

type signData struct {
	ProcessID types.ProcessID
	MsgType   string
	Raw       messages.RawMsg
}

func (b *signedEchoBroadcast) handleBroadcastedMsg(msg ByzConsistenceMessage) {
	if b.sentEcho {
		return
	}

	if msg.From() != b.sender {
		return
	}

	data := signData{
		ProcessID: b.self,
		MsgType:   "ECHO",
		Raw:       msg.RawMsg,
	}

	bytes, err := json.Marshal(data)
	if err != nil {
		return
	}

	sign, err := b.identify.Sign(bytes)
	if err != nil {
		return
	}

	echoMsg := ByzConsistenceSignedEchoMessage{
		BaseMsg: messages.NewBase(uuid.New(), b.self, ByzConsistenceSignedEchoMessageName),
		Inner:   msg.RawMsg,
		Sign:    sign,
	}

	b.sentEcho = true
	b.al.Send(b.sender, echoMsg)
}

func (b *signedEchoBroadcast) handleEchoMsg(msg ByzConsistenceSignedEchoMessage) {
	_, exists := b.echos[msg.From()]
	if exists {
		return
	}

	data := signData{
		ProcessID: msg.From(),
		MsgType:   "ECHO",
		Raw:       msg.Inner,
	}

	bytes, err := json.Marshal(data)
	if err != nil {
		return
	}

	if err = b.identify.Verify(msg.From(), bytes, msg.Sign); err != nil {
		return
	}

	b.echos[msg.From()] = msg
}

func (b *signedEchoBroadcast) handleFinal(msg ByzConsistenceFinalMessage) {
	if b.delivered {
		return
	}

	validCount := 0
	for from, other := range b.echos {
		sign := other.Sign

		data := signData{
			ProcessID: from,
			MsgType:   "ECHO",
			Raw:       msg.Inner,
		}

		bytes, err := json.Marshal(data)
		if err != nil {
			return
		}

		if err = b.identify.Verify(from, bytes, sign); err != nil {
			continue
		}
		validCount++

		if validCount > (len(b.processes)+b.faults)/2 {
			innerMsgTyp, err := b.registry.Unmarshal(msg.Inner.Raw, msg.Inner.RawType)
			if err != nil {
				return
			}

			innerMsg, ok := innerMsgTyp.(types.Message)
			if !ok {
				return
			}

			b.delivered = true
			go b.Deliverer.Deliver(innerMsg)
			return
		}
	}
}

func (b *signedEchoBroadcast) alBroadcast(msg types.Message) {
	for _, p := range b.processes {
		b.al.Send(p, msg)
	}
}

func (b *signedEchoBroadcast) maybeBroadcastFinal() {
	if b.sentFinal {
		return
	}

	for from, msg := range b.echos {
		processesCount := 0

		for fromOther, other := range b.echos {
			if from == fromOther {
				continue
			}
			if !msg.BaseMsg.Equal(other.BaseMsg) {
				continue
			}
			if !msg.Inner.Equal(other.Inner) {
				continue
			}

			processesCount++

			if processesCount > (len(b.processes)+b.faults)/2 {
				b.sentFinal = true
				finalMsg := ByzConsistenceFinalMessage{
					BaseMsg: messages.NewBase(uuid.New(), b.self, ByzConsistenceFinalMessageName),
					Inner:   msg.Inner,
					Signs:   make([]ByzConsistenceFinalMessageSign, 0),
				}

				for from, other := range b.echos {
					sign := other.Sign
					finalMsg.Signs = append(finalMsg.Signs, ByzConsistenceFinalMessageSign{
						Sign:      sign,
						ProcessID: from,
					})
				}
				return
			}
		}
	}
}
