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
	"slices"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

type Scheme struct {
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
	evts             chan fsm.Event
	output           chan types.Value
	stopCh           chan struct{}
	beb              broadcaster.Broadcaster
	logger           zerolog.Logger
}

func StartScheme(
	ctx context.Context,
	self types.ProcessID,
	ts int,
	processes map[types.ProcessID]struct{},
	crashFaultsCount int,
	domain []types.Value,
	beb broadcaster.Broadcaster,
	logger *zerolog.Logger,
) *Scheme {
	c := makeScheme(ctx, self, ts, processes, crashFaultsCount, domain, beb, logger)
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
) *Scheme {
	c := new(Scheme)
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

	if logger != nil {
		c.logger = *logger
	}
	return c
}

func (c *Scheme) start() {
	c.triggerEvt(commitEvt{})
	go c.eventLoop()
}

func (c *Scheme) eventLoop() {
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

func (c *Scheme) handleEvent(event fsm.Event) error {
	switch evt := event.(type) {
	case commitEvt:
		return c.commit()
	case commitedEvt:
		c.onCommit(evt.from, evt.commit)
		return nil
	case revealEvt:
		return c.onReveal(evt.from, evt.value, evt.salt)
	default:
		return errors.New("unknown event")
	}
}

func (c *Scheme) onCommit(from types.ProcessID, commit []byte) {
	c.commits[from] = commit

	if len(c.commits) >= c.processesCount-c.crashFaultsCount {
		msg := RevealMsg{
			BaseMsg: messages.NewBase(uuid.New(), c.self, "reveal"),
			Value:   c.value,
			Salt:    c.salt,
		}
		c.broadcast(msg)
	}
}

func (c *Scheme) commit() error {
	salt := c.genSalt()
	c.salt = salt
	value := c.randValueFromDomain()
	c.value = value

	commit, err := c.buildCommit(value, salt)
	if err != nil {
		return fmt.Errorf("build commit: %w", err)
	}

	msg := CommitMsg{
		BaseMsg: messages.NewBase(uuid.New(), c.self, "commit"),
		Commit:  commit,
	}

	c.broadcast(msg)

	return nil
}

func (c *Scheme) onReveal(from types.ProcessID, val types.Value, salt []byte) error {
	_, exists := c.processes[from]
	if !exists {
		return fmt.Errorf("undefined process: %s", from.String())
	}

	receivedCommit, ok := c.commits[from]
	if !ok {
		return fmt.Errorf("no commit for process: %s", from.String())
	}

	commit, err := c.buildCommit(val, salt)
	if err != nil {
		return fmt.Errorf("build commit: %w", err)
	}

	if bytes.Compare(commit, receivedCommit) != 0 {
		return fmt.Errorf("commit mismatch for process: %s", from.String())
	}

	c.values[from] = val

	if len(c.values) >= c.processesCount-c.crashFaultsCount {
		val, err := c.aggregate()
		if err != nil {
			return fmt.Errorf("aggregate: %w", err)
		}
		c.sendOutput(val)
	}

	return nil
}

func (c *Scheme) aggregate() (types.Value, error) {
	initVal := 1 << 8
	initValBytes := make([]byte, 512)
	binary.BigEndian.PutUint64(initValBytes, uint64(initVal))

	values := utils.ValuesSlice(c.values)
	slices.SortFunc(values, func(a, b types.Value) int {
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

	res := int(binary.BigEndian.Uint32(initValBytes))

	valuesCount := len(values)
	resIdx := res % valuesCount

	return values[resIdx], nil
}

func (c *Scheme) buildCommit(val types.Value, salt []byte) ([]byte, error) {
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

func (c *Scheme) triggerEvt(evt fsm.Event) {
	select {
	case c.evts <- evt:
	case <-c.ctx.Done():
	}
}

func (c *Scheme) genSalt() []byte {
	b := make([]byte, 256)
	_, _ = rand.Read(b)
	return b
}

func (c *Scheme) randValueFromDomain() types.Value {
	randomIndex := mrand.Int32N(int32(len(c.domain)))
	return c.domain[randomIndex]
}

func (c *Scheme) Deliver(msg types.Message) {
	switch m := msg.(type) {
	case CommitMsg:
		go c.triggerEvt(commitedEvt{
			BaseEvent: fsm.NewBaseEvent("commited"),
			commit:    m.Commit,
			from:      m.From(),
		})
	case RevealMsg:
		go c.triggerEvt(revealEvt{
			BaseEvent: fsm.NewBaseEvent("reveal"),
			from:      m.From(),
			value:     m.Value,
			salt:      m.Salt,
		})
	}
}

func (c *Scheme) broadcast(msg types.Message) {
	schemeMsg := SchemeMsg{
		BaseMsg: messages.NewBase(uuid.New(), c.self, "commit_scheme"),
		Ts:      c.id,
		Inner:   msg,
	}

	c.beb.Broadcast(c.ctx, schemeMsg)
}

func (c *Scheme) sendOutput(v types.Value) {
	defer close(c.output)
	defer c.cancel()

	select {
	case c.output <- v:
	case <-c.ctx.Done():
	}
}
