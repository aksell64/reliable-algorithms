package uniform

import (
	"context"
	"reliable/broadcaster"
	"reliable/logger"
	"reliable/messages"
	"reliable/p2p"
	"reliable/types"
	"reliable/utils"
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
	ts  int
	val *types.Value
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
	evts             chan types.Event
	stopCh           chan struct{}
	stopOnce         sync.Once
	aborted          chan AbortedState
	sm               *types.StateMachine
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
	ec.evts = make(chan types.Event, 100)

	if logger != nil {
		ec.logger = *logger
	}

	ec.sm = types.NewStateMachine()
	initMachine(ec, ec.sm)

	return ec
}

func (ec *epochConsensus) StartEpoch(
	ctx context.Context,
	leader types.ProcessID,
	epoch int,
	current State,
) {
	ec.ctx, ec.cancel = context.WithCancel(ctx)

	ec.beb.AddDeliverer(ec, types.DelivererWithMsgNames(ReadMsgName, WriteMsgName, DecideMsgName))
	ec.pl.AddDeliverer(ec, types.DelivererWithMsgNames(StateMsgName, AcceptMsgName))

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

	ec.apply(initEvent{
		epochTs: epoch,
		leader:  leader,
		current: current,
	})

	ec.logger.Info().Any("state", ec.current).Msg("epoch started")
}

func (ec *epochConsensus) Propose(v types.Value) {
	ec.apply(proposeEvent{val: v})
}

func (ec *epochConsensus) Abort() {
	ec.stopOnce.Do(ec.abort)
}

func (ec *epochConsensus) abort() {
	ec.cancel()
	<-ec.stopCh
	aState := AbortedState{Ts: ec.epochTs, State: &ec.current}
	ec.aborted <- aState
	ec.logger.Warn().Any("state", aState.State).Msg("aborted")
	ec.doCleanup()
}

func (ec *epochConsensus) doCleanup() {
	close(ec.aborted)
	close(ec.decided)
	ec.pl.RemoveDeliverer(ec)
	ec.beb.RemoveDeliverer(ec)
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

func (ec *epochConsensus) onInit(evt initEvent) types.State {
	ec.epochTs = evt.epochTs
	ec.leader = evt.leader
	ec.current = evt.current
	if ec.leader == ec.self {
		return StatePropose
	}
	return StateIdle
}

func (ec *epochConsensus) onPropose(evt proposeEvent) types.State {
	ec.logger.Info().
		Str("val", evt.val.String()).
		Msg("start proposing")

	val := evt.val.Copy()
	ec.tempValue = &val

	msg := makeReadMsg(ec.self)
	ec.beb.Broadcast(ec.ctx, msg)

	ec.logger.Info().Msg("start reading")

	return StateReading
}

func (ec *epochConsensus) onReceivedRead(evt receivedReadEvent) types.State {
	ec.receivedStates[evt.msg.From()] = State{
		ts:  evt.msg.Ts,
		val: evt.msg.Val,
	}

	val := evt.msg.Val
	if val != nil {
		v := *val
		ec.logger.Info().
			Int("ts", evt.msg.Ts).
			Str("val", v.String()).
			Str("from", evt.msg.From().String()).
			Any("received", utils.KeysSlice(ec.receivedStates)).
			Msg("received read")
	} else {
		ec.logger.Info().
			Int("ts", evt.msg.Ts).
			Str("val", "empty").
			Str("from", evt.msg.From().String()).
			Any("received", utils.KeysSlice(ec.receivedStates)).
			Msg("received read")
	}

	if len(ec.receivedStates) < ec.quorum() {
		return StateReading
	}

	highest := ec.highestState()
	if highest.val != nil {
		ec.tempValue = highest.val
	}

	ec.receivedStates = make(map[types.ProcessID]State)

	msg := makeWriteMsg(ec.self, ec.tempValue)

	ec.beb.Broadcast(ec.ctx, msg)

	ec.logger.Info().Any("val", ec.tempValue).Msg("start writing")

	return StateWriting
}

func (ec *epochConsensus) onReceivedAccept(evt receivedAcceptEvent) types.State {
	ec.receivedAccepted++
	ec.logger.Info().
		Int("curAcceptCount", ec.receivedAccepted).
		Msg("received accept")

	if ec.receivedAccepted < ec.quorum() {
		return StateWriting
	}

	msg := makeDecidedMsg(ec.self, *ec.tempValue)
	ec.beb.Broadcast(ec.ctx, msg)

	ec.logger.Info().Msg("start deciding")

	return StateDeciding
}

func (ec *epochConsensus) onDecided(evt decidedEvent) types.State {
	ec.stopOnce.Do(func() {
		ec.decided <- evt.val
		ec.cancel()
		go func() {
			<-ec.stopCh
			ec.doCleanup()
		}()
	})
	return StateDecided
}

func (ec *epochConsensus) onHandleRead(evt handleReadEvent) types.State {
	current := ec.current
	msg := makeStateMsg(ec.self, current.ts, current.val)
	ec.pl.Send(evt.from, msg)
	return types.SameState
}

func (ec *epochConsensus) onHandleWrite(evt handleWriteEvent) types.State {
	ec.current.ts = ec.epochTs
	ec.current.val = evt.v
	msg := makeAcceptMsg(ec.self)
	ec.pl.Send(evt.from, msg)
	return types.SameState
}

func (ec *epochConsensus) quorum() int {
	return ec.processesCount/2 + 1
}

func (ec *epochConsensus) highestState() State {
	var maxTsState State
	for _, state := range ec.receivedStates {
		if state.ts >= maxTsState.ts {
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
		ec.handleAccept()
	case ReadMsg:
		ec.handleRead(msg.From())
	case WriteMsg:
		ec.handleWrite(msg.From(), msg.Val)
	case DecidedMsg:
		ec.handleDecide(msg.From(), msg.Val)
	}
}

func (ec *epochConsensus) handleState(msg StateMsg) {
	if ec.leader != ec.self {
		return
	}
	ec.apply(receivedReadEvent{msg: msg})
}

func (ec *epochConsensus) handleAccept() {
	if ec.leader != ec.self {
		return
	}
	ec.apply(receivedAcceptEvent{})
}

func (ec *epochConsensus) handleRead(from types.ProcessID) {
	if ec.leader != from {
		return
	}
	ec.apply(handleReadEvent{from: from})
}

func (ec *epochConsensus) handleWrite(from types.ProcessID, val *types.Value) {
	if ec.leader != from {
		return
	}
	ec.apply(handleWriteEvent{from: from, v: val})
}

func (ec *epochConsensus) handleDecide(from types.ProcessID, v types.Value) {
	if ec.leader != from {
		return
	}
	ec.apply(decidedEvent{val: v})
}

func (ec *epochConsensus) apply(evt types.Event) {
	select {
	case <-ec.ctx.Done():
	case ec.evts <- evt:
	}
}

func makeStateMsg(from types.ProcessID, ts int, val *types.Value) StateMsg {
	return StateMsg{
		Message: messages.NewBase(uuid.New(), from, StateMsgName),
		Ts:      ts,
		Val:     val,
	}
}

func makeAcceptMsg(from types.ProcessID) AcceptMsg {
	return AcceptMsg{
		Message: messages.NewBase(uuid.New(), from, AcceptMsgName),
	}
}

func makeDecidedMsg(from types.ProcessID, val types.Value) DecidedMsg {
	return DecidedMsg{
		Message: messages.NewBase(uuid.New(), from, DecideMsgName),
		Val:     val,
	}
}

func makeReadMsg(from types.ProcessID) ReadMsg {
	return ReadMsg{
		Message: messages.NewBase(uuid.New(), from, ReadMsgName),
	}
}

func makeWriteMsg(from types.ProcessID, val *types.Value) WriteMsg {
	return WriteMsg{
		Message: messages.NewBase(uuid.New(), from, WriteMsgName),
		Val:     val,
	}
}
