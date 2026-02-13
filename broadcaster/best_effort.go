package broadcaster

import (
	"context"
	"reliable/logger"
	"reliable/p2p"
	"reliable/types"
	"sync"

	"github.com/rs/zerolog"
)

type bestEffortBroadcaster struct {
	types.Deliverer
	mu       sync.RWMutex
	self     types.ProcessID
	nodes    map[types.ProcessID]struct{}
	links    p2p.Link
	selector BroadcastNodeSelector
	logger   zerolog.Logger
	once     *types.WorkerOnce
}

func NewBestEffortBroadcaster(
	self types.ProcessID,
	processes []types.ProcessID,
	selector BroadcastNodeSelector,
	links p2p.Link,
) Broadcaster {
	bcast := new(bestEffortBroadcaster)
	bcast.logger = logger.Logger().With().Str("bcaster", "beb").Logger()
	bcast.nodes = make(map[types.ProcessID]struct{})
	bcast.self = self
	bcast.selector = selector
	bcast.Deliverer = types.NewUnaryDeliverer(self)
	bcast.once = types.NewWorkerOnce()

	links.AddDeliverer(bcast)
	bcast.links = links

	for _, pid := range processes {
		bcast.AddCorrect(pid)
	}

	return bcast
}

func (b *bestEffortBroadcaster) Init() {
	b.once.Init(func() {
		b.links.Init()
	})
}

func (b *bestEffortBroadcaster) Start() {
	b.once.Start(func() {
		b.links.Start()
	})
}

func (b *bestEffortBroadcaster) Stop() {
	b.once.Stop(func() {
		b.links.Stop()
	})
}

func (b *bestEffortBroadcaster) Broadcast(ctx context.Context, msg types.Message) {
	b.mu.RLock()
	nodes := make(map[types.ProcessID]struct{})
	for id, _ := range b.nodes {
		nodes[id] = struct{}{}
	}
	b.mu.RUnlock()

	for pid, _ := range nodes {
		if ctx.Err() != nil {
			return
		}

		b.links.Send(pid, msg)
	}
}

func (b *bestEffortBroadcaster) AddCorrect(id types.ProcessID) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.nodes[id] = struct{}{}
}

func (b *bestEffortBroadcaster) RemoveCorrect(id types.ProcessID) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.nodes, id)
}

func (b *bestEffortBroadcaster) Deliver(msg types.Message) {
	//b.logger.Info().Str("msg", msg.Name()).Str("from", msg.From().String()).Msg("delivering message")
	b.Deliverer.Deliver(msg)
}

func (b *bestEffortBroadcaster) Instance() string { return "beb" }
