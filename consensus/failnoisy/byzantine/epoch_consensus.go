package byzantine

import (
	"context"
	"fmt"
	"reliable/logger"
	"reliable/messages"
	"reliable/p2p"
	"reliable/types"
	"reliable/types/fsm"
	"reliable/utils/codec"
	"sync"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

type State struct {
	Epoch    int
	Value    *types.Value
	WriteSet WriteSet
}

type WriteSet struct {
	Snapshots []Snapshot
}

type Snapshot struct {
	Epoch int
	Value *types.Value
}

type AbortedState struct {
	Ts    int
	State *State
}

type readWriteConsensus struct {
	types.Deliverer
	ctx            context.Context
	cancel         context.CancelFunc
	al             p2p.Link
	self           types.ProcessID
	processes      []types.ProcessID
	processesCount int
	faults         int
	epoch          int
	initEpoch      int
	leader         types.ProcessID
	state          State
	accepted       map[types.ProcessID]types.Value
	acceptedCounts map[types.Value]int
	written        map[types.ProcessID]types.Value
	writtenCounts  map[types.Value]int
	errChan        chan error
	decideCh       chan types.Value
	evtsCh         chan fsm.Event
	abortedCh      chan AbortedState
	collector      ConditionsCollector
	registry       *codec.Registry
	stopOnce       sync.Once
	stopCh         chan struct{}
	logger         zerolog.Logger
}

func newEpochConsensus(
	ctx context.Context,
	al p2p.Link,
	self types.ProcessID,
	processes []types.ProcessID,
	faults int,
	initEpoch int,
	registry *codec.Registry,
	collector ConditionsCollector,
	l *zerolog.Logger,
) *readWriteConsensus {
	c := new(readWriteConsensus)
	c.ctx, c.cancel = context.WithCancel(ctx)
	c.Deliverer = types.NewUnaryDeliverer(self)
	c.al = al
	c.al.AddDeliverer(c)
	c.self = self
	c.processes = processes
	c.faults = faults
	c.initEpoch = initEpoch
	c.registry = registry
	c.collector = collector
	if l != nil {
		c.logger = logger.NewNodeScopeLoggerFrom(*l, logger.Scope{"epoch_consensus", "rw"})
	} else {
		c.logger = logger.NewNodeScopeLogger(self, logger.Scope{"epoch_consensus", "rw"})
	}
	c.accepted = make(map[types.ProcessID]types.Value)
	c.acceptedCounts = make(map[types.Value]int)
	c.written = make(map[types.ProcessID]types.Value)
	c.writtenCounts = make(map[types.Value]int)
	c.evtsCh = make(chan fsm.Event, 100)
	c.errChan = make(chan error, 1)
	c.decideCh = make(chan types.Value, 1)
	c.abortedCh = make(chan AbortedState, 1)
	c.stopCh = make(chan struct{}, 1)
	return c
}

func (c *readWriteConsensus) StartEpoch(
	ctx context.Context,
	leader types.ProcessID,
	epoch int,
	current State,
) {
	c.ctx, c.cancel = context.WithCancel(ctx)

	c.epoch = epoch
	c.leader = leader
	c.state = current
	go c.eventLoop()
}

func (c *readWriteConsensus) Propose(v types.Value) {
	c.triggerApply(newProposeEvt(v))
}

func (c *readWriteConsensus) Abort() {
	c.stopOnce.Do(c.abort)
}

func (c *readWriteConsensus) Aborted() <-chan AbortedState {
	return c.abortedCh
}

func (c *readWriteConsensus) Decided() chan types.Value {
	return c.decideCh
}

func (c *readWriteConsensus) Epoch() int {
	return c.epoch
}

func (c *readWriteConsensus) eventLoop() {
	for {
		select {
		case <-c.ctx.Done():
			return
		case err := <-c.errChan:
			c.handleErr(err)
		default:
		}

		select {
		case <-c.ctx.Done():
		case evt := <-c.evtsCh:
			switch event := evt.(type) {
			case proposeEvt:
				c.handlePropose(event)
			case readEvt:
				c.handleRead(event)
			case collectedEvt:
				c.handleCollected(event)
			case writeEvt:
				c.handleWrite(event)
			case acceptEvt:
				c.handleAccept(event)
			default:
				c.triggerErr(fmt.Errorf("unknown event: %v", event))
			}
		}
	}
}

func (c *readWriteConsensus) runCollectorInput(val []byte) {
	err := c.collector.Input(val)
	if err != nil {
		c.triggerErr(err)
	}
}

func (c *readWriteConsensus) handlePropose(evt proposeEvt) {
	if c.leader != c.self {
		return
	}

	cpValue := evt.val.Copy()
	if c.state.Value == nil {
		c.state.Value = &cpValue
	}

	msg := NewRwReadMsg(c.self)
	go func() {
		for _, p := range c.processes {
			c.al.Send(p, msg)
		}
	}()
}

func (c *readWriteConsensus) handleRead(evt readEvt) {
	if c.leader != evt.from {
		return
	}

	stateRaw, err := c.encodeState()
	if err != nil {
		c.triggerErr(fmt.Errorf("handle read: %w", err))
		return
	}

	err = c.collector.Input(stateRaw)
	if err != nil {
		c.triggerErr(err)
	}
}

func (c *readWriteConsensus) handleCollected(evt collectedEvt) {
	states := evt.states
	for _, state := range states {
		val := state.Value
		if val == nil {
			continue
		}
		epoch := state.Epoch

		if c.checkBinds(epoch, val, states) {
			c.startWrite(*val)
			return
		}
	}

	if c.checkUnbound(states) {
		for _, state := range states {
			val := state.Value
			if val == nil {
				continue
			}
			for _, other := range states {
				if other.Value == val {
					c.startWrite(*val)
					return
				}
			}
		}
	}
}

func (c *readWriteConsensus) startWrite(temp types.Value) {
	for idx, snap := range c.state.WriteSet.Snapshots {
		if snap.Value == nil {
			continue
		}
		if *snap.Value == temp {
			snapshots := append(c.state.WriteSet.Snapshots[:idx], c.state.WriteSet.Snapshots[idx+1:]...)
			c.state.WriteSet = WriteSet{snapshots}
		}
	}
	c.state.WriteSet.Snapshots = append(c.state.WriteSet.Snapshots, Snapshot{
		Epoch: c.epoch,
		Value: &temp,
	})

	c.broadcastWrite(temp)
}

func (c *readWriteConsensus) handleWrite(evt writeEvt) {
	if val, exists := c.written[evt.from]; exists && evt.val == val {
		return
	}

	c.written[evt.from] = evt.val
	c.writtenCounts[evt.val] = c.writtenCounts[evt.val] + 1

	var (
		shouldAccept bool
		acceptVal    types.Value
	)
	for val, count := range c.writtenCounts {
		if count <= c.byzantineQuorum() {
			continue
		}
		shouldAccept = true
		acceptVal = val
		break
	}

	if !shouldAccept {
		return
	}

	raw, err := c.registry.Marshal(acceptVal)
	if err != nil {
		c.triggerErr(fmt.Errorf("handle write: marshal accepted: %w", err))
		return
	}

	msg := RWAcceptMsg{
		BaseMsg:  messages.NewBase(uuid.New(), c.self, RWAcceptMsgName),
		ValueRaw: messages.NewRaw(raw, acceptVal.Type()),
	}

	go func() {
		for _, p := range c.processes {
			c.al.Send(p, msg)
		}
	}()
}

func (c *readWriteConsensus) handleAccept(evt acceptEvt) {
	if val, ok := c.accepted[evt.from]; ok && val == evt.val {
		return
	}
	c.accepted[evt.from] = evt.val
	c.acceptedCounts[evt.val] = c.acceptedCounts[evt.val] + 1

	var (
		shouldDecide bool
		decideVal    types.Value
	)
	for val, count := range c.acceptedCounts {
		if count <= c.byzantineQuorum() {
			continue
		}
		shouldDecide = true
		decideVal = val
		break
	}

	if !shouldDecide {
		return
	}

	go c.decide(decideVal)
}

func (c *readWriteConsensus) checkBinds(epoch int, val *types.Value, states []State) bool {
	if len(states) > c.processesCount-c.faults {
		return false
	}

	return c.checkQuorumHighest(epoch, val, states) && c.checkCertified(epoch, val, states)
}

func (c *readWriteConsensus) checkQuorumHighest(epoch int, val *types.Value, states []State) bool {
	var (
		countDefined int
		exists       bool
	)
	for _, state := range states {
		if state.Epoch == epoch && state.Value == val {
			exists = true
			countDefined++
		}
		if state.Epoch < epoch {
			countDefined++
		}
	}

	if exists && countDefined > c.byzantineQuorum() {
		return true
	}
	return false
}

func (c *readWriteConsensus) checkCertified(epoch int, val *types.Value, states []State) bool {
	count := 0
	for _, state := range states {
		for _, snap := range state.WriteSet.Snapshots {
			if snap.Value == val && snap.Epoch <= epoch {
				count++
			}
		}
	}

	return count > c.processesCount-c.faults
}

func (c *readWriteConsensus) checkUnbound(states []State) bool {
	if len(states) > c.processesCount-c.faults {
		return false
	}
	count := 0
	for _, state := range states {
		if state.Epoch == c.initEpoch {
			count++
		}
	}
	return count > c.byzantineQuorum()
}

func (c *readWriteConsensus) byzantineQuorum() int {
	return (c.processesCount + c.faults) / 2
}

func (c *readWriteConsensus) triggerErr(err error) {
	select {
	case c.errChan <- err:
	case <-c.ctx.Done():
	default:
		go func() {
			select {
			case c.errChan <- err:
			case <-c.ctx.Done():
			}
		}()
	}
}

func (c *readWriteConsensus) broadcastWrite(val types.Value) {
	raw, err := c.registry.Marshal(val)
	if err != nil {
		c.triggerErr(fmt.Errorf("marshal write: %w", err))
		return
	}

	rawMsg := messages.NewRaw(raw, RWWriteMsgName)
	msg := NewRWWriteMsg(c.self, rawMsg)

	go func() {
		for _, p := range c.processes {
			c.al.Send(p, msg)
		}
	}()
}

func (c *readWriteConsensus) handleErr(err error) {
	c.logger.Error().Err(err).Msg("eventLoop")
}

func (c *readWriteConsensus) decide(v types.Value) {
	c.stopOnce.Do(func() {
		c.decideCh <- v
		c.cancel()
		<-c.stopCh
		c.doCleanup()
	})
}

func (c *readWriteConsensus) abort() {
	c.cancel()
	<-c.stopCh

	aState := AbortedState{Ts: c.epoch, State: &c.state}
	c.abortedCh <- aState
	c.doCleanup()
}

func (c *readWriteConsensus) doCleanup() {
	close(c.abortedCh)
	close(c.decideCh)
	close(c.evtsCh)
}

func (c *readWriteConsensus) checkSound(states []State) bool {
	if c.checkUnbound(states) {
		return true
	}

	for _, state := range states {
		epoch, val := state.Epoch, state.Value
		if c.checkBinds(epoch, val, states) {
			return true
		}
	}

	return false
}

func (c *readWriteConsensus) ValidateCollected(sent []Sent) error {
	states, err := c.parseSentStates(sent)
	if err != nil {
		return fmt.Errorf("parse: %w", err)
	}

	return c.validateStates(states)
}

func (c *readWriteConsensus) parseSentStates(sent []Sent) ([]State, error) {
	states := make([]State, 0, len(sent))
	for _, raw := range sent {
		if raw.IsUndefined() {
			continue
		}
		state, err := c.decodeState(raw.Msg)
		if err != nil {
			return nil, fmt.Errorf("decode: %w", err)
		}
		states = append(states, state)
	}
	return states, nil
}

func (c *readWriteConsensus) validateStates(states []State) error {
	if !c.checkSound(states) {
		return fmt.Errorf("sound invalid")
	}

	return nil
}

func (c *readWriteConsensus) OnCollected(sent []Sent) error {
	states, err := c.parseSentStates(sent)
	if err != nil {
		return fmt.Errorf("parse: %w", err)
	}

	if err = c.validateStates(states); err != nil {
		return fmt.Errorf("validate states: %w", err)
	}

	c.triggerApply(newCollectedEvt(states))
	return nil
}

func (c *readWriteConsensus) triggerApply(evt fsm.Event) {
	select {
	case c.evtsCh <- evt:
	case <-c.ctx.Done():
	}
}

func (c *readWriteConsensus) encodeState() ([]byte, error) {
	snaps := make([]RWSnapshot, 0)
	for _, snap := range c.state.WriteSet.Snapshots {
		valRaw, err := c.encodeValueRaw(snap.Value)
		if err != nil {
			return nil, fmt.Errorf("encode snap value: %w", err)
		}
		snaps = append(snaps, RWSnapshot{
			Epoch:    snap.Epoch,
			ValueRaw: valRaw,
		})
	}

	valRaw, err := c.encodeValueRaw(c.state.Value)
	if err != nil {
		return nil, fmt.Errorf("encode value: %w", err)
	}

	msg := RWStateMsg{
		ValueRaw:    valRaw,
		ValueEpoch:  c.state.Epoch,
		WriteSetRaw: snaps,
	}

	raw, err := c.registry.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("encode state msg: %w", err)
	}

	return raw, nil
}

func (c *readWriteConsensus) encodeValueRaw(val *types.Value) (messages.RawMsg, error) {
	var valueRaw messages.RawMsg
	if val != nil {
		v := *val
		raw, err := c.registry.Marshal(v)
		if err != nil {
			return messages.RawMsg{}, fmt.Errorf("marshal value")
		}
		valueRaw.Raw = raw
		valueRaw.RawType = v.Type()
	} else {
		valueRaw.RawType = codec.UndefinedType
	}
	return valueRaw, nil
}

func (c *readWriteConsensus) decodeState(raw []byte) (State, error) {
	msgObj, err := c.registry.Unmarshal(raw, RWStateMsgName)
	if err != nil {
		return State{}, fmt.Errorf("unmarshal state msg: %w", err)
	}

	msg, ok := msgObj.(RWStateMsg)
	if !ok {
		return State{}, fmt.Errorf("cast state msg")
	}

	snaps := make([]Snapshot, 0, len(msg.WriteSetRaw))
	for _, snapRaw := range msg.WriteSetRaw {
		value, err := c.decodeValueRaw(snapRaw.ValueRaw)
		if err != nil {
			return State{}, fmt.Errorf("unmarshal snap value raw: %w", err)
		}
		snaps = append(snaps, Snapshot{
			Epoch: snapRaw.Epoch,
			Value: value,
		})
	}

	value, err := c.decodeValueRaw(msg.ValueRaw)
	if err != nil {
		return State{}, fmt.Errorf("unmarshal value raw: %w", err)
	}

	state := State{
		Epoch:    msg.ValueEpoch,
		Value:    value,
		WriteSet: WriteSet{Snapshots: snaps},
	}

	return state, nil
}

func (c *readWriteConsensus) decodeValueRaw(v messages.RawMsg) (*types.Value, error) {
	if v.RawType != codec.UndefinedType {
		valObj, err := c.registry.Unmarshal(v.Raw, v.RawType)
		if err != nil {
			return nil, fmt.Errorf("unmarshal value raw: %w", err)
		}

		val, ok := valObj.(types.Value)
		if !ok {
			return nil, fmt.Errorf("cast value raw")
		}
		return &val, nil
	}
	return nil, nil
}

func (c *readWriteConsensus) Deliver(m types.Message) {
	var err error
	switch msg := m.(type) {
	case RWReadMsg:
		c.triggerApply(newReadEvt(msg.From()))
	case RWWriteMsg:
		err = c.handleWriteDelivered(msg)
	case RWAcceptMsg:
		err = c.handleAcceptDelivered(msg)
	}
	if err != nil {
		c.logger.Error().Err(err).Msg("handle deliverer")
	}
}

func (c *readWriteConsensus) handleWriteDelivered(msg RWWriteMsg) error {
	valObj, err := c.registry.Unmarshal(msg.ValueRaw.Raw, msg.ValueRaw.RawType)
	if err != nil {
		return fmt.Errorf("handle write: unmarshal value raw: %w", err)
	}
	val, ok := valObj.(types.Value)
	if !ok {
		return fmt.Errorf("handle write: cast val")
	}

	c.triggerApply(newWriteEvt(msg.From(), val))
	return nil
}

func (c *readWriteConsensus) handleAcceptDelivered(msg RWAcceptMsg) error {
	valObj, err := c.registry.Unmarshal(msg.ValueRaw.Raw, msg.ValueRaw.RawType)
	if err != nil {
		return fmt.Errorf("handle accept: unmarshal value raw: %w", err)
	}
	val, ok := valObj.(types.Value)
	if !ok {
		return fmt.Errorf("handle accept: cast val")
	}

	c.triggerApply(newAcceptEvt(msg.From(), val))
	return nil
}
