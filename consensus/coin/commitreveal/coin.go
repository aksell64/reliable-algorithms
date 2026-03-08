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
	current          *scheme
	mu               sync.Mutex
	logger           zerolog.Logger
	registry         *codec.Registry
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
) coin.TsCoinScheme {
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
	c.registry = registry

	return c
}

func (c *Coin) SetReceiver(receiver coin.Receiver) {
	c.receiver = receiver
}

func (c *Coin) RunScheme(ts int, domain []types.Value) {
	c.mu.Lock()
	c.ts = ts
	inboxed, err := c.buffered.GetAndClear(inbox.NewStringKey(genTsMsgKey(ts)))
	if err != nil {
		c.logger.Error().Err(err).Msg("failed to clear buffered message")
	}

	pendings := make([]types.Message, 0, len(inboxed))
	for _, i := range inboxed {
		msg, ok := i.(types.Message)
		if !ok {
			c.logger.Error().Err(err).Msg("failed to cast message")
			continue
		}
		pendings = append(pendings, msg)
	}

	s := startScheme(c.ctx, c.self, c.ts, c.processes, c.crashFaultsCount, domain, c.beb, &c.logger, c.registry)
	c.current = s
	c.mu.Unlock()

	for _, msg := range pendings {
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

	innerObj, err := c.registry.Unmarshal(smsg.Raw, smsg.RawType)
	if err != nil {
		c.logger.Error().Err(err).Msg("unmarshal message")
		return
	}

	inner, ok := innerObj.(types.Message)
	if !ok {
		c.logger.Error().Err(err).Msg("cast message")
		return
	}

	saveToInbox := func() {
		inboxKey := inbox.NewStringKey(genTsMsgKey(smsg.Ts))
		c.buffered.Store(inboxKey, inner)
		c.logger.Info().
			Int("curTs", c.ts).
			Int("ts", smsg.Ts).
			Str("inner", inner.Name()).
			Msg("buffered msg")
	}

	c.mu.Lock()

	switch {
	case smsg.Ts == c.ts && c.current == nil:
		saveToInbox()
		c.mu.Unlock()
	case smsg.Ts > c.ts:
		saveToInbox()
		c.mu.Unlock()
	case smsg.Ts == c.ts && c.current != nil:
		c.mu.Unlock()
		c.current.Deliver(inner)
	default:
		c.logger.Warn().Msg("dropped msg")
	}
}

func (c *Coin) registerCodec(registry *codec.Registry) {
	codec.RegisterTyped[CommitMsg](registry)
	codec.RegisterTyped[RevealMsg](registry)
	codec.RegisterTyped[SchemeMsg](registry)
}

func genTsMsgKey(ts int) string {
	return fmt.Sprintf("coin_cr_msg_ts%d", ts)
}
