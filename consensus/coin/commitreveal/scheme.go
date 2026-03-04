package commitreveal

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	mrand "math/rand/v2"
	"reliable/broadcaster"
	"reliable/messages"
	"reliable/types"
	"reliable/types/fsm"
	"reliable/utils"
	"reliable/utils/codec"
	"slices"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

type reveal struct {
	value types.Value
	salt  []byte
}

type scheme struct {
	ctx              context.Context
	cancel           context.CancelFunc
	self             types.ProcessID
	id               int
	hasher           hash.Hash
	processes        map[types.ProcessID]struct{}
	processesCount   int
	crashFaultsCount int
	salt             []byte
	domain           []types.Value
	value            types.Value
	values           map[types.ProcessID]types.Value
	commits          map[types.ProcessID][]byte
	pendingReveals   map[types.ProcessID]reveal
	evts             chan fsm.Event
	output           chan types.Value
	stopCh           chan struct{}
	registry         *codec.Registry
	beb              broadcaster.Broadcaster
	logger           zerolog.Logger
}

func startScheme(
	ctx context.Context,
	self types.ProcessID,
	ts int,
	processes map[types.ProcessID]struct{},
	crashFaultsCount int,
	domain []types.Value,
	beb broadcaster.Broadcaster,
	logger *zerolog.Logger,
	registry *codec.Registry,
) *scheme {
	c := makeScheme(ctx, self, ts, processes, crashFaultsCount, domain, beb, logger, registry)
	c.start()
	return c
}

func makeScheme(ctx context.Context,
	self types.ProcessID,
	ts int,
	processes map[types.ProcessID]struct{},
	crashFaultsCount int,
	domain []types.Value,
	beb broadcaster.Broadcaster,
	logger *zerolog.Logger,
	registry *codec.Registry,
) *scheme {
	c := new(scheme)
	c.ctx, c.cancel = context.WithCancel(ctx)
	c.self = self
	c.id = ts
	c.commits = make(map[types.ProcessID][]byte)
	c.processes = processes
	c.values = make(map[types.ProcessID]types.Value)
	c.beb = beb
	c.domain = domain
	c.evts = make(chan fsm.Event, 50)
	c.crashFaultsCount = crashFaultsCount
	c.processesCount = len(processes)
	c.hasher = sha256.New()
	c.output = make(chan types.Value, 1)
	c.stopCh = make(chan struct{})
	c.pendingReveals = make(map[types.ProcessID]reveal)
	c.registry = registry

	if logger != nil {
		c.logger = *logger
	}
	return c
}

func (c *scheme) start() {
	c.triggerEvt(commitEvt{})
	go c.eventLoop()
}

func (c *scheme) eventLoop() {
	defer close(c.stopCh)
	for c.ctx.Err() == nil {
		select {
		case <-c.ctx.Done():
			return
		case event := <-c.evts:
			err := c.handleEvent(event)
			if err != nil {
				c.logger.Err(err).Msg("handling event")
			}
		}
	}
}

func (c *scheme) handleEvent(event fsm.Event) error {
	switch evt := event.(type) {
	case commitEvt:
		return c.commit()
	case commitedEvt:
		return c.onCommit(evt.from, evt.commit)
	case revealEvt:
		return c.onReveal(evt.from, evt.value, evt.salt)
	default:
		return errors.New("unknown event")
	}
}

func (c *scheme) onCommit(from types.ProcessID, commit []byte) error {
	c.commits[from] = commit
	if reveal, existsReveal := c.pendingReveals[from]; existsReveal {
		go c.triggerEvt(revealEvt{
			BaseEvent: fsm.NewBaseEvent("reveal"),
			from:      from,
			value:     reveal.value,
			salt:      reveal.salt,
		})
	}

	if len(c.commits) >= c.processesCount-c.crashFaultsCount {
		rawValue, err := c.registry.Marshal(c.value)
		if err != nil {
			return fmt.Errorf("marshaling reveal value: %w", err)
		}

		msg := RevealMsg{
			BaseMsg:  messages.NewBase(uuid.New(), c.self, RevealMsgName),
			ValueRaw: messages.NewRaw(rawValue, c.value.Type()),
			Salt:     c.salt,
		}
		return c.broadcast(msg)
	}

	return nil
}

func (c *scheme) commit() error {
	salt := c.genSalt()
	c.salt = salt
	value := c.randValueFromDomain()
	c.value = value

	commit, err := c.buildCommit(value, salt)
	if err != nil {
		return fmt.Errorf("build commit: %w", err)
	}

	msg := CommitMsg{
		BaseMsg: messages.NewBase(uuid.New(), c.self, CommitMsgName),
		Commit:  commit,
	}

	c.logger.Info().Str("val", value.String()).Bytes("salt", salt[:5]).Msg("commit")

	return c.broadcast(msg)
}

func (c *scheme) onReveal(from types.ProcessID, val types.Value, salt []byte) error {
	_, exists := c.processes[from]
	if !exists {
		return fmt.Errorf("undefined process: %s", from.String())
	}

	receivedCommit, ok := c.commits[from]
	if !ok {
		c.pendingReveals[from] = reveal{
			salt:  salt,
			value: val,
		}
		return fmt.Errorf("no commit for process: %s, save to pending", from.String())
	}

	commit, err := c.buildCommit(val, salt)
	if err != nil {
		return fmt.Errorf("build commit: %w", err)
	}

	if bytes.Compare(commit, receivedCommit) != 0 {
		return fmt.Errorf("commit mismatch for process: %s", from.String())
	}

	c.values[from] = val

	c.logger.Info().
		Str("val", val.String()).
		Bytes("salt", salt[:5]).
		Int("count received", len(c.values)).
		Int("need", c.processesCount-c.crashFaultsCount).
		Msg("reveal received")

	if len(c.values) >= c.processesCount-c.crashFaultsCount {
		val, err := c.aggregate()
		if err != nil {
			return fmt.Errorf("aggregate: %w", err)
		}
		c.sendOutput(val)
	}

	return nil
}

func (c *scheme) aggregate() (types.Value, error) {
	var initVal uint64 = 1<<64 - 1
	initValBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(initValBytes, initVal)

	values := utils.ValuesSlice(c.values)
	slices.SortStableFunc(values, func(a, b types.Value) int {
		if a.Compare(b) {
			return 0
		}
		if a.Less(b) {
			return -1
		}
		return 1
	})

	for _, val := range values {
		raw, err := val.Bytes()
		if err != nil {
			return nil, fmt.Errorf("get raw val: %w", err)
		}
		initValBytes = utils.XORBytes(initValBytes, raw)
	}

	resIdx := binary.BigEndian.Uint64(initValBytes) % uint64(len(values))

	result := values[resIdx]

	c.logger.Info().Str("result", result.String()).Msg("aggregate")

	return result, nil
}

func (c *scheme) buildCommit(val types.Value, salt []byte) ([]byte, error) {
	defer c.hasher.Reset()
	rawValue, err := val.Bytes()
	if err != nil {
		return nil, fmt.Errorf("serialize value: %w", err)
	}

	c.hasher.Write(salt)
	c.hasher.Write(rawValue)
	commit := c.hasher.Sum(nil)
	return commit, err
}

func (c *scheme) triggerEvt(evt fsm.Event) {
	select {
	case c.evts <- evt:
	case <-c.ctx.Done():
	}
}

func (c *scheme) genSalt() []byte {
	b := make([]byte, 256)
	_, _ = rand.Read(b)
	return b
}

func (c *scheme) randValueFromDomain() types.Value {
	randomIndex := mrand.Int32N(int32(len(c.domain)))
	return c.domain[randomIndex]
}

func (c *scheme) Deliver(msg types.Message) {
	switch m := msg.(type) {
	case CommitMsg:
		go c.triggerEvt(commitedEvt{
			BaseEvent: fsm.NewBaseEvent("commited"),
			commit:    m.Commit,
			from:      m.From(),
		})
	case RevealMsg:
		valueObj, err := c.registry.Unmarshal(m.ValueRaw.Raw, m.ValueRaw.RawType)
		if err != nil {
			c.logger.Error().Err(err).Msg("unmarshal reveal value")
			return
		}

		value, ok := valueObj.(types.Value)
		if !ok {
			c.logger.Error().Err(err).Msg("cast reveal value")
			return
		}

		go c.triggerEvt(revealEvt{
			BaseEvent: fsm.NewBaseEvent("reveal"),
			from:      m.From(),
			value:     value,
			salt:      m.Salt,
		})
	}
}

func (c *scheme) broadcast(msg types.Message) error {
	raw, err := c.registry.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal msg: %w", err)
	}

	schemeMsg := SchemeMsg{
		BaseMsg: messages.NewBase(uuid.New(), c.self, SchemeMsgName),
		Ts:      c.id,
		RawMsg:  messages.NewRaw(raw, msg.Type()),
	}

	c.beb.Broadcast(c.ctx, schemeMsg)

	return nil
}

func (c *scheme) sendOutput(v types.Value) {
	defer close(c.output)
	defer c.cancel()

	select {
	case c.output <- v:
	case <-c.ctx.Done():
	}
}
