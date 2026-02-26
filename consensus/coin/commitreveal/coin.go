package commitreveal

import (
	"context"
	"fmt"
	"hash"
	"reliable/broadcaster"
	"reliable/consensus/coin"
	"reliable/logger"
	"reliable/types"
	"reliable/types/inbox"
	"reliable/utils/codec"
	"sync"

	"github.com/rs/zerolog"
)

type Coin struct {
	types.Deliverer
	ctx              context.Context
	self             types.ProcessID
	hasher           hash.Hash
	processes        map[types.ProcessID]struct{}
	processesCount   int
	crashFaultsCount int
	beb              broadcaster.Broadcaster
	buffered         *inbox.Inbox
	receiver         coin.Receiver
	ts               int
	current          *Scheme
	mu               sync.Mutex
	logger           zerolog.Logger
}

func NewCoin(
	ctx context.Context,
	self types.ProcessID,
	hasher hash.Hash,
	processes []types.ProcessID,
	processesCount int,
	crashFaultsCount int,
	beb broadcaster.Broadcaster,
	inbox *inbox.Inbox,
	registry *codec.Registry,
	log *zerolog.Logger,
) *Coin {
	c := new(Coin)
	c.ctx = ctx
	c.Deliverer = types.NewUnaryDeliverer(self)
	c.self = self
	c.hasher = hasher
	c.processes = map[types.ProcessID]struct{}{}
	for _, pid := range processes {
		c.processes[pid] = struct{}{}
	}

	c.processesCount = processesCount
	c.crashFaultsCount = crashFaultsCount
	if log != nil {
		c.logger = logger.NewNodeScopeLoggerFrom(*log, logger.Scope{"coin", "commit_reveal"})
	} else {
		c.logger = logger.NewNodeScopeLogger(self, logger.Scope{"coin", "commit_reveal"})
	}
	c.buffered = inbox

	c.beb = beb
	c.beb.AddDeliverer(c)

	c.registerCodec(registry)

	return c
}

func (c *Coin) SetReceiver(receiver coin.Receiver) {
	c.receiver = receiver
}

func (c *Coin) RunScheme(ts int, domain []types.Value) {
	c.mu.Lock()
	c.ts = ts
	pendings, err := c.buffered.GetAndClear(inbox.String(genTsMsgKey(ts)))
	if err != nil {
		c.logger.Error().Err(err).Msg("failed to clear buffered message")
	}
	s := StartScheme(c.ctx, c.self, c.ts, c.processes, c.crashFaultsCount, domain, c.beb, &c.logger)
	c.current = s
	c.mu.Unlock()

	for _, val := range pendings {
		msg, ok := val.(types.Message)
		if !ok {
			c.logger.Error().Err(err).Msg("failed to cast message")
			continue
		}
		s.Deliver(msg)
	}

	select {
	case <-c.ctx.Done():
	case v, ok := <-s.output:
		if !ok {
			return
		}
		if c.receiver != nil {
			c.receiver.ReceiveCoinFlip(v, ts)
		}
	}
}

func (c *Coin) Deliver(msg types.Message) {
	smsg, ok := msg.(SchemeMsg)
	if !ok {
		return
	}

	c.mu.Lock()
	if smsg.Ts > c.ts {
		inboxKey := inbox.String(genTsMsgKey(smsg.Ts))
		c.buffered.Store(inboxKey, smsg.Inner)
		c.mu.Unlock()
	} else if smsg.Ts == c.ts && c.current != nil {
		c.mu.Unlock()
		c.current.Deliver(smsg.Inner)
	}
}

func (c *Coin) registerCodec(registry *codec.Registry) {
	codec.RegisterTyped[CommitMsg](registry)
	codec.RegisterTyped[RevealMsg](registry)
}

func genTsMsgKey(ts int) string {
	return fmt.Sprintf("msg_ts%d", ts)
}
