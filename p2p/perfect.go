package p2p

import (
	"reliable/logger"
	"reliable/types"
	"sync"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

type perfectP2PLinks struct {
	types.Deliverer
	self      types.ProcessID
	sl        Link
	delivered map[uuid.UUID]struct{}
	mu        sync.RWMutex
	logger    zerolog.Logger
	once      *types.WorkerOnce
}

func NewPerfectP2PLinks(
	self types.ProcessID,
	sl Link,
) Link {
	l := &perfectP2PLinks{
		self:      self,
		sl:        sl,
		delivered: make(map[uuid.UUID]struct{}),
		logger:    logger.NewNodeScopeLogger(self, logger.Scope{"p2p", "pl"}),
		Deliverer: types.NewUnaryDeliverer(self),
		once:      types.NewWorkerOnce(),
	}

	sl.AddDeliverer(l)
	l.sl = sl

	return l
}

func (l *perfectP2PLinks) Init() {
	l.once.Init(func() {
		l.sl.Init()
	})
}

func (l *perfectP2PLinks) Start() {
	l.once.Start(func() {
		l.sl.Start()
	})
}

func (l *perfectP2PLinks) Stop() {
	l.once.Stop(func() {
		l.sl.Stop()
	})
}

func (l *perfectP2PLinks) Send(to types.ProcessID, msg types.Message) {
	l.sl.Send(to, msg)
}

func (l *perfectP2PLinks) Deliver(msg types.Message) {
	l.mu.RLock()
	_, delivered := l.delivered[msg.ID()]
	l.mu.RUnlock()

	if delivered {
		return
	}

	l.mu.Lock()
	_, delivered = l.delivered[msg.ID()]
	if delivered {
		l.mu.Unlock()
		return
	}
	l.delivered[msg.ID()] = struct{}{}
	l.mu.Unlock()

	//l.logger.Info().Str("msg", msg.Name()).Str("from", msg.From().String()).Msg("delivering message")
	l.Deliverer.Deliver(msg)
}

func (l *perfectP2PLinks) Instance() string {
	return "pl"
}
