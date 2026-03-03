package broadcaster

import (
	"context"
	"reliable/failure"
	"reliable/logger"
	"reliable/messages"
	"reliable/types"
	"reliable/utils"
	"reliable/utils/codec"
	"sync"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

type lazyReliableBroadcaster struct {
	types.Deliverer
	ctx      context.Context
	self     types.ProcessID
	correct  map[types.ProcessID]struct{}
	beb      Broadcaster
	pfd      *failure.PerfectFailureDetector
	received map[types.ProcessID]map[uuid.UUID]types.Message
	mu       sync.RWMutex
	logger   zerolog.Logger
	registry *codec.Registry
	once     *types.WorkerOnce
}

func NewLazyReliableBroadcaster(
	ctx context.Context,
	self types.ProcessID,
	beb Broadcaster,
	pfd *failure.PerfectFailureDetector,
	registry *codec.Registry,
) Broadcaster {
	rb := &lazyReliableBroadcaster{
		Deliverer: types.NewUnaryDeliverer(self),
		ctx:       ctx,
		self:      self,
		pfd:       pfd,
		correct:   make(map[types.ProcessID]struct{}),
		received:  make(map[types.ProcessID]map[uuid.UUID]types.Message),
		logger:    logger.NewNodeScopeLogger(self, logger.Scope{"bcaster", "rb"}),
		once:      types.NewWorkerOnce(),
		registry:  registry,
	}

	beb.AddDeliverer(rb)
	rb.beb = beb

	codec.RegisterTyped[ReliableBroadcastMessage](registry)

	return rb
}

func (b *lazyReliableBroadcaster) Init() {
	b.once.Init(func() {
		b.beb.Init()
		b.pfd.Subscribe(b)
	})
}

func (b *lazyReliableBroadcaster) Start() {
	b.once.Start(func() {
		b.beb.Start()
	})
}

func (b *lazyReliableBroadcaster) Stop() {}

func (b *lazyReliableBroadcaster) AddCorrect(pid types.ProcessID) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.correct[pid] = struct{}{}
	b.beb.AddCorrect(pid)
}

func (b *lazyReliableBroadcaster) RemoveCorrect(id types.ProcessID) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.correct, id)
	b.beb.RemoveCorrect(id)
}

func (b *lazyReliableBroadcaster) Broadcast(ctx context.Context, msg types.Message) {
	raw, err := b.registry.Marshal(msg)
	if err != nil {
		b.logger.Error().Err(err).Msg("marshal inner msg")
		return
	}

	bmsg := ReliableBroadcastMessage{
		BaseMsg: messages.NewBase(uuid.New(), b.self, ReliableBroadcastMessageName),
		RawMsg:  messages.NewRaw(raw, msg.Type()),
	}
	b.beb.Broadcast(ctx, bmsg)
}

func (b *lazyReliableBroadcaster) Deliver(msg types.Message) {
	bmsg, ok := msg.(ReliableBroadcastMessage)
	if !ok {
		return
	}

	innerObj, err := b.registry.Unmarshal(bmsg.Raw, bmsg.RawType)
	if err != nil {
		b.logger.Error().Err(err).Msg("unmarshal inner msg")
		return
	}

	innerMsg, ok := innerObj.(types.Message)
	if !ok {
		b.logger.Error().Msg("convert inner msg")
		return
	}

	b.mu.Lock()
	_, exists := b.received[msg.From()]
	if !exists {
		b.received[msg.From()] = make(map[uuid.UUID]types.Message)
	}

	_, exists = b.received[msg.From()][msg.ID()]
	if exists {
		b.mu.Unlock()
		return
	}
	b.mu.Unlock()

	//b.logger.Info().Str("msg", msg.Name()).Str("from", msg.From().String()).Msg("delivering message")
	b.Deliverer.Deliver(innerMsg)

	b.mu.Lock()
	b.received[msg.From()][msg.ID()] = msg
	_, isCorrect := b.correct[msg.From()]
	if isCorrect {
		b.mu.Unlock()
		return
	}
	b.mu.Unlock()

	b.beb.Broadcast(b.ctx, msg)
}

func (b *lazyReliableBroadcaster) OnCrash(pid types.ProcessID) {
	b.mu.Lock()
	if _, exists := b.correct[pid]; !exists {
		b.mu.Unlock()
		return
	}

	delete(b.correct, pid)
	msgsMap, ok := b.received[pid]
	if !ok {
		b.mu.Unlock()
		return
	}
	msgs := utils.ValuesSlice(msgsMap)
	b.mu.Unlock()

	for _, msg := range msgs {
		b.beb.Broadcast(b.ctx, msg)
	}
}

func (b *lazyReliableBroadcaster) Instance() string { return "rb" }

type eagerReliableBroadcaster struct {
	types.Deliverer
	ctx       context.Context
	self      types.ProcessID
	beb       Broadcaster
	delivered map[uuid.UUID]struct{}
	mu        sync.Mutex
	once      *types.WorkerOnce
	registry  *codec.Registry
}

func NewEagerReliableBroadcaster(
	ctx context.Context,
	self types.ProcessID,
	processes []types.ProcessID,
	beb Broadcaster,
	registry *codec.Registry,
) Broadcaster {
	rb := &eagerReliableBroadcaster{
		ctx:       ctx,
		self:      self,
		beb:       beb,
		delivered: make(map[uuid.UUID]struct{}),
		Deliverer: types.NewUnaryDeliverer(self),
		once:      types.NewWorkerOnce(),
	}

	beb.AddDeliverer(rb)

	for _, p := range processes {
		rb.AddCorrect(p)
	}

	codec.RegisterTyped[ReliableBroadcastMessage](registry)

	return rb
}

func (b *eagerReliableBroadcaster) Init() {
	b.once.Init(func() {
		b.beb.Init()
	})
}

func (b *eagerReliableBroadcaster) Start() {
	b.once.Start(func() {
		b.beb.Start()
	})
}

func (b *eagerReliableBroadcaster) Stop() {}

func (b *eagerReliableBroadcaster) AddCorrect(pid types.ProcessID) {
	b.beb.AddCorrect(pid)
}

func (b *eagerReliableBroadcaster) RemoveCorrect(id types.ProcessID) {
	b.beb.RemoveCorrect(id)
}

func (b *eagerReliableBroadcaster) Broadcast(ctx context.Context, msg types.Message) {
	raw, err := b.registry.Marshal(msg)
	if err != nil {
		return
	}

	bmsg := ReliableBroadcastMessage{
		BaseMsg: messages.NewBase(uuid.New(), b.self, ReliableBroadcastMessageName),
		RawMsg:  messages.NewRaw(raw, msg.Type()),
	}
	b.beb.Broadcast(ctx, bmsg)
}

func (b *eagerReliableBroadcaster) Deliver(msg types.Message) {
	bmsg, ok := msg.(ReliableBroadcastMessage)
	if !ok {
		return
	}

	innerObj, err := b.registry.Unmarshal(bmsg.Raw, bmsg.RawType)
	if err != nil {
		return
	}

	innerMsg, ok := innerObj.(types.Message)
	if !ok {
		return
	}

	b.mu.Lock()
	if _, exists := b.delivered[bmsg.ID()]; exists {
		b.mu.Unlock()
		return
	}
	b.delivered[bmsg.ID()] = struct{}{}
	b.mu.Unlock()

	b.Deliverer.Deliver(innerMsg)
	b.beb.Broadcast(b.ctx, bmsg)
}

func (b *eagerReliableBroadcaster) OnCrash(pid types.ProcessID) {}

func (b *eagerReliableBroadcaster) ProcessID() types.ProcessID {
	return b.self
}

func (b *eagerReliableBroadcaster) Instance() string { return "rb" }
