package uniform

import (
	"context"
	"reliable/broadcaster"
	"reliable/logger"
	"reliable/messages"
	"reliable/p2p"
	"reliable/types"
	"reliable/types/fsm"
	"strconv"
	"sync"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

type AbortedState struct {
	Ts    int
	State *State
}

type State struct {
	Ts  int
	Val *types.Value
}

// epochConsensus is Read/Write Epoch Consensus instance
type epochConsensus struct {
	types.Deliverer
	ctx              context.Context
	cancel           context.CancelFunc
	self             types.ProcessID
	tempValue        *types.Value
	current          State
	receivedStates   map[types.ProcessID]State
	receivedAccepted int
	leader           types.ProcessID
	epochTs          int
	beb              broadcaster.Broadcaster
	pl               p2p.Link
	processesCount   int
	decided          chan types.Value
	evts             chan fsm.Event
	stopCh           chan struct{}
	stopOnce         sync.Once
	aborted          chan AbortedState
	sm               *fsm.StateMachine
	logger           zerolog.Logger
}

func newEpochConsensus(
	self types.ProcessID,
	beb broadcaster.Broadcaster,
	pl p2p.Link,
	processesCount int,
	logger *zerolog.Logger,
) *epochConsensus {
	ec := new(epochConsensus)
	ec.self = self
	ec.Deliverer = types.NewUnaryDeliverer(self)
	ec.beb = beb
	ec.pl = pl
	ec.processesCount = processesCount
	ec.evts = make(chan fsm.Event, 100)

	if logger != nil {
		ec.logger = *logger
	}

	ec.sm = fsm.NewStateMachine()
	initMachine(ec, ec.sm)

	return ec
}

func (ec *epochConsensus) Epoch() int {
	return ec.epochTs
}

func (ec *epochConsensus) StartEpoch(
	ctx context.Context,
	leader types.ProcessID,
	epoch int,
	current State,
) {
	ec.ctx, ec.cancel = context.WithCancel(ctx)

	ec.logger = logger.NewNodeScopeLoggerFrom(ec.logger,
		logger.Scope{"leader", leader.String()},
		logger.Scope{"etc", strconv.Itoa(epoch)},
		logger.Scope{"consensus", "rw"})

	ec.receivedStates = make(map[types.ProcessID]State)
	ec.stopCh = make(chan struct{})
	ec.decided = make(chan types.Value, 1)
	ec.aborted = make(chan AbortedState, 1)

	ec.sm.SetState(StateIdle)

	go ec.eventLoop()

	ec.triggerApply(initEvent{
		epochTs: epoch,
		leader:  leader,
		current: current,
	})
}

func (ec *epochConsensus) Propose(v types.Value) {
	ec.triggerApply(proposeEvent{val: v})
}

func (ec *epochConsensus) Abort() {
	ec.stopOnce.Do(ec.abort)
}

func (ec *epochConsensus) abort() {
	ec.cancel()
	<-ec.stopCh

	ec.drainEvents()
	aState := AbortedState{Ts: ec.epochTs, State: &ec.current}
	ec.aborted <- aState
	ec.doCleanup()
}

func (ec *epochConsensus) decide(val types.Value) {
	ec.stopOnce.Do(func() {
		ec.decided <- val
		ec.cancel()
		<-ec.stopCh
		ec.doCleanup()
	})
}

func (ec *epochConsensus) doCleanup() {
	close(ec.aborted)
	close(ec.decided)
}

func (ec *epochConsensus) Aborted() <-chan AbortedState {
	return ec.aborted
}

func (ec *epochConsensus) eventLoop() {
	defer close(ec.stopCh)
	for ec.ctx.Err() == nil {
		select {
		case <-ec.ctx.Done():
			return
		case evt := <-ec.evts:
			ec.sm.Apply(evt)
		}
	}
}

func (ec *epochConsensus) drainEvents() {
	for {
		select {
		case evt := <-ec.evts:
			ec.sm.Apply(evt)
		default:
			return
		}
	}
}

func (ec *epochConsensus) onInit(evt initEvent) fsm.State {
	ec.epochTs = evt.epochTs
	ec.leader = evt.leader
	ec.current = evt.current
	ec.logger.Info().Any("state", ec.current).Msg("epoch started")
	if ec.leader == ec.self {
		return StatePropose
	}
	return StateIdle
}

func (ec *epochConsensus) onPropose(evt proposeEvent) fsm.State {
	ec.logger.Info().
		Str("val", evt.val.String()).
		Msg("start proposing")

	val := evt.val.Copy()
	ec.tempValue = &val

	msg := makeReadMsg(ec.self, ec.epochTs)
	ec.beb.Broadcast(ec.ctx, msg)

	ec.logger.Info().Msg("start reading")

	return StateReading
}

func (ec *epochConsensus) onReceivedRead(evt receivedReadEvent) fsm.State {
	ec.receivedStates[evt.msg.From()] = State{
		Ts:  evt.msg.Ts,
		Val: evt.msg.Val,
	}

	val := evt.msg.Val
	ec.logger.Info().
		Int("ts", evt.msg.Ts).
		Any("val", val).
		Str("from", evt.msg.From().String()).
		Any("receivedCount", len(ec.receivedStates)).
		Msg("received read")

	if len(ec.receivedStates) < ec.quorum() {
		return StateReading
	}

	highest := ec.highestState()
	if highest.Val != nil {
		ec.tempValue = highest.Val
	}

	ec.receivedStates = make(map[types.ProcessID]State)

	msg := makeWriteMsg(ec.self, ec.epochTs, ec.tempValue)
	ec.beb.Broadcast(ec.ctx, msg)

	ec.logger.Info().Any("val", ec.tempValue).Msg("start writing")

	return StateWriting
}

func (ec *epochConsensus) onReceivedAccept(evt receivedAcceptEvent) fsm.State {
	ec.receivedAccepted++
	ec.logger.Info().
		Int("curAcceptCount", ec.receivedAccepted).
		Str("from", evt.from.String()).
		Msg("received accept")

	if ec.receivedAccepted < ec.quorum() {
		return StateWriting
	}

	msg := makeDecidedMsg(ec.self, ec.epochTs, *ec.tempValue)
	ec.beb.Broadcast(ec.ctx, msg)

	ec.logger.Info().Msg("start deciding")

	return StateDeciding
}

func (ec *epochConsensus) onDecided(evt decidedEvent) fsm.State {
	ec.decide(evt.val)
	return StateDecided
}

func (ec *epochConsensus) onHandleRead(evt handleReadEvent) fsm.State {
	current := ec.current
	msg := makeStateMsg(ec.self, current.Ts, current.Val, ec.epochTs)
	ec.logger.Info().Any("state", ec.current).Str("to", evt.from.String()).Msg("send read")
	ec.pl.Send(evt.from, msg)
	return fsm.SameState
}

func (ec *epochConsensus) onHandleWrite(evt handleWriteEvent) fsm.State {
	ec.current.Ts = ec.epochTs
	ec.current.Val = evt.v
	ec.logger.Info().Any("val", evt.v).Str("from", evt.from.String()).Msg("written")
	msg := makeAcceptMsg(ec.self, ec.epochTs)
	ec.pl.Send(evt.from, msg)
	return fsm.SameState
}

func (ec *epochConsensus) quorum() int {
	return ec.processesCount/2 + 1
}

func (ec *epochConsensus) highestState() State {
	var maxTsState State
	for _, state := range ec.receivedStates {
		if state.Ts >= maxTsState.Ts {
			maxTsState = state
		}
	}
	return maxTsState
}

func (ec *epochConsensus) Decided() chan types.Value {
	return ec.decided
}

func (ec *epochConsensus) Deliver(message types.Message) {
	switch msg := message.(type) {
	case StateMsg:
		ec.handleState(msg)
	case AcceptMsg:
		ec.handleAccept(msg.From(), msg.Epoch)
	case ReadMsg:
		ec.handleRead(msg.From(), msg.Epoch)
	case WriteMsg:
		ec.handleWrite(msg.From(), msg.Val, msg.Epoch)
	case DecidedMsg:
		ec.handleDecide(msg.From(), msg.Val, msg.Epoch)
	}
}

func (ec *epochConsensus) handleState(msg StateMsg) {
	if ec.leader != ec.self || ec.epochTs != msg.Epoch {
		return
	}
	ec.triggerApply(receivedReadEvent{msg: msg})
}

func (ec *epochConsensus) handleAccept(from types.ProcessID, epoch int) {
	if ec.leader != ec.self || ec.epochTs != epoch {
		return
	}
	ec.triggerApply(receivedAcceptEvent{from: from})
}

func (ec *epochConsensus) handleRead(from types.ProcessID, epoch int) {
	if ec.leader != from || ec.epochTs != epoch {
		return
	}
	ec.triggerApply(handleReadEvent{from: from})
}

func (ec *epochConsensus) handleWrite(from types.ProcessID, val *types.Value, epoch int) {
	if ec.leader != from || ec.epochTs != epoch {
		return
	}
	ec.triggerApply(handleWriteEvent{from: from, v: val})
}

func (ec *epochConsensus) handleDecide(from types.ProcessID, v types.Value, epoch int) {
	if ec.leader != from || ec.epochTs != epoch {
		return
	}
	ec.triggerApply(decidedEvent{val: v})
}

func (ec *epochConsensus) triggerApply(evt fsm.Event) {
	select {
	case <-ec.ctx.Done():
	case ec.evts <- evt:
	}
}

func makeStateMsg(from types.ProcessID, ts int, val *types.Value, epoch int) StateMsg {
	return StateMsg{
		Message: messages.NewBase(uuid.New(), from, StateMsgName),
		Ts:      ts,
		Val:     val,
		Epoch:   epoch,
	}
}

func makeAcceptMsg(from types.ProcessID, epoch int) AcceptMsg {
	return AcceptMsg{
		Message: messages.NewBase(uuid.New(), from, AcceptMsgName),
		Epoch:   epoch,
	}
}

func makeDecidedMsg(from types.ProcessID, epoch int, val types.Value) DecidedMsg {
	return DecidedMsg{
		Message: messages.NewBase(uuid.New(), from, DecideMsgName),
		Val:     val,
		Epoch:   epoch,
	}
}

func makeReadMsg(from types.ProcessID, epoch int) ReadMsg {
	return ReadMsg{
		Message: messages.NewBase(uuid.New(), from, ReadMsgName),
		Epoch:   epoch,
	}
}

func makeWriteMsg(from types.ProcessID, epoch int, val *types.Value) WriteMsg {
	return WriteMsg{
		Message: messages.NewBase(uuid.New(), from, WriteMsgName),
		Val:     val,
		Epoch:   epoch,
	}
}
