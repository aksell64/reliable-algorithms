package uniform

import (
	"context"
	"reliable/broadcaster"
	"reliable/logger"
	"reliable/p2p"
	"reliable/types"
	"reliable/utils"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

const (
	proposeInterval = 100 * time.Millisecond
)

type newEpochEnvelope struct {
	ts     int
	leader types.ProcessID
}

type decideEnvelope struct {
	ts  int
	val types.Value
}

type abortedEnvelope struct {
	force bool
	state AbortedState
}

type EpochConsensus interface {
	StartEpoch(
		ctx context.Context,
		leader types.ProcessID,
		epoch int,
		current State,
	)
	Epoch() int
	Propose(value types.Value)
	Abort()
	Aborted() <-chan AbortedState
	Decided() chan types.Value
	Deliver(msg types.Message)
}

type EpochConsensusFactory func(
	self types.ProcessID,
	beb broadcaster.Broadcaster,
	pl p2p.Link,
	pcCount int,
	logger *zerolog.Logger) EpochConsensus

type Node struct {
	types.Deliverer
	ctx            context.Context
	cancel         context.CancelFunc
	self           types.ProcessID
	processesCount int
	processes      map[types.ProcessID]types.ProcessRank
	ecFactory      EpochConsensusFactory
	inner          EpochConsensus
	epochChanger   *LeaderBasedEpochChanger
	newTs          int
	newLeader      types.ProcessID
	ets            int
	leader         types.ProcessID
	leaderRank     types.ProcessRank
	val            *types.Value
	proposed       bool
	mu             sync.RWMutex
	decidedEnvsCh  chan decideEnvelope
	decideCh       chan types.Value
	decided        bool
	decidedVal     types.Value
	aborted        chan abortedEnvelope
	state          *State
	newEpochCh     chan newEpochEnvelope
	inbox          *msgsInbox
	wg             sync.WaitGroup
	pl             p2p.Link
	beb            broadcaster.Broadcaster
	once           *types.WorkerOnce
	stopCh         chan struct{}
	msgsCh         chan types.Message
	logger         zerolog.Logger
}

func New(
	ctx context.Context,
	self types.ProcessID,
	processes map[types.ProcessID]types.ProcessRank,
	changer *LeaderBasedEpochChanger,
	pl p2p.Link,
	beb broadcaster.Broadcaster,
) *Node {
	n := new(Node)
	n.ctx, n.cancel = context.WithCancel(ctx)
	n.self = self
	n.processes = processes
	n.Deliverer = types.NewUnaryDeliverer(self)
	n.pl = pl
	n.beb = beb
	n.epochChanger = changer
	n.newEpochCh = make(chan newEpochEnvelope, 50)
	n.decidedEnvsCh = make(chan decideEnvelope, 1)
	n.decideCh = make(chan types.Value, 1)
	n.aborted = make(chan abortedEnvelope, 1)
	n.stopCh = make(chan struct{})
	n.once = types.NewWorkerOnce()
	n.logger = logger.NewNodeScopeLogger(self, logger.Scope{"consensus", "uni"})
	n.inbox = newMsgsInbox()
	n.msgsCh = make(chan types.Message, 100)
	n.ecFactory = n.newEC

	n.beb.AddDeliverer(n, types.DelivererWithMsgNames(ReadMsgName, WriteMsgName, DecideMsgName))
	n.pl.AddDeliverer(n, types.DelivererWithMsgNames(StateMsgName, AcceptMsgName))

	return n
}

func (n *Node) Init() {
	n.once.Init(func() {
		types.InitWorkers(n.epochChanger, n.pl, n.beb)
		n.val = nil
		n.proposed = false
		n.ets = 0
		selfRank := n.processes[n.self]
		n.leader = n.self
		n.leaderRank = selfRank
		n.processesCount = len(n.processes)

		for pid, rank := range n.processes {
			if n.leaderRank >= rank {
				n.leaderRank = rank
				n.leader = pid
			}
		}
	})
}

func (n *Node) Start() {
	n.once.Start(func() {

		n.epochChanger.SetEpochStarter(n)
		types.StartWorkers(n.epochChanger, n.pl, n.beb)

		n.startEpoch()
		go n.background()
	})
}

func (n *Node) Stop() {
	n.stopOnce()
}

func (n *Node) stopOnce() {
	n.once.Stop(func() {
		n.cancel()
		n.wg.Wait()
		close(n.stopCh)
		n.pl.RemoveDeliverer(n)
		n.beb.RemoveDeliverer(n)
		n.logger.Info().Msg("stopped")
	})
}

func (n *Node) Propose(v types.Value) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.val = utils.Ptr(v.Copy())
}

func (n *Node) Decided() <-chan types.Value {
	return n.decideCh
}

func (n *Node) Crashed() chan struct{} {
	return n.stopCh
}

func (n *Node) StartEpoch(ts int, leader types.ProcessID) {
	n.triggerNewEpoch(ts, leader)
}

func (n *Node) newEC(
	self types.ProcessID,
	beb broadcaster.Broadcaster,
	pl p2p.Link,
	count int,
	logger *zerolog.Logger) EpochConsensus {
	return newEpochConsensus(self, beb, pl, count, logger)
}

func (n *Node) triggerNewEpoch(ts int, leader types.ProcessID) {
	env := newEpochEnvelope{
		ts:     ts,
		leader: leader,
	}

	select {
	case <-n.ctx.Done():
	case n.newEpochCh <- env:
	}
}

func (n *Node) background() {
	defer n.stopOnce()
	proposeTimer := time.NewTimer(proposeInterval)
	defer proposeTimer.Stop()
	var proposeChecked bool

	epochLogTicker := time.NewTicker(20 * time.Second)
	defer epochLogTicker.Stop()

	for {
		proposeChecked = false
		select {
		case <-n.ctx.Done():
			return

		case <-epochLogTicker.C:
			n.logger.Info().Int("ets", n.ets).Msg("epoch log")

		case <-proposeTimer.C:
			n.maybePropose()
			proposeTimer.Reset(proposeInterval)
			proposeChecked = true

		case msg := <-n.msgsCh:
			n.deliver(msg)

		case decided := <-n.decidedEnvsCh:
			if decided.ts != n.ets || n.decided {
				continue
			}

			n.logger.Info().
				Str("val", decided.val.String()).
				Int("ts", decided.ts).Msg("decided")

			n.decideCh <- decided.val
			close(n.decideCh)
			n.decided = true
			n.decidedVal = decided.val

		case abortedEnv := <-n.aborted:
			abortState := abortedEnv.state
			if abortState.Ts != n.ets {
				n.inbox.clear(abortState.Ts)
				continue
			}

			n.logger.Warn().
				Int("stateEts", abortState.Ts).
				Any("stateTs", abortState.State.Ts).
				Any("stateVal", abortState.State.Val).
				Bool("force", abortedEnv.force).
				Msg("aborted")

			n.state = abortState.State
			n.ets = n.newTs
			n.leader = n.newLeader
			n.proposed = false
			n.startEpoch()
			n.inbox.clear(abortState.Ts)

		case newEpoch := <-n.newEpochCh:
			if newEpoch.ts <= n.ets {
				continue
			}

			n.newTs, n.newLeader = newEpoch.ts, newEpoch.leader
			if n.decided {
				evt := AbortedState{
					Ts: n.ets,
					State: &State{
						Ts:  n.ets,
						Val: &n.decidedVal,
					},
				}
				go n.triggerAborted(evt)
				continue
			}

			if n.inner != nil {
				inner := n.inner
				n.inner = nil
				inner.Abort()
			}
		}

		if !proposeChecked {
			n.maybePropose()
		}
	}
}

func (n *Node) triggerAborted(state AbortedState) {
	env := abortedEnvelope{
		state: state,
		force: true,
	}
	select {
	case <-n.ctx.Done():
	case n.aborted <- env:
	}
}

func (n *Node) maybePropose() {
	n.mu.Lock()
	if n.leader == n.self && !n.proposed && n.val != nil {
		n.proposed = true
		val := *n.val
		n.mu.Unlock()
		n.inner.Propose(val)
		n.logger.Info().Int("ets", n.ets).Str("val", val.String()).Msg("proposed")
		return
	}
	n.mu.Unlock()
}

func (n *Node) startEpoch() {
	n.inner = n.ecFactory(n.self, n.beb, n.pl, n.processesCount /*utils.Ptr(zerolog.Nop())*/, &n.logger)

	var state State
	if n.state != nil {
		state = *n.state
	}
	n.inner.StartEpoch(n.ctx, n.leader, n.ets, state)

	bufferedMsgs := n.inbox.buffered(n.ets)

	for _, msg := range bufferedMsgs {
		n.inner.Deliver(msg)
	}

	n.wg.Add(1)
	go n.waitFinishEpoch(n.inner)
}

func (n *Node) waitFinishEpoch(inner EpochConsensus) {
	defer n.wg.Done()

	abortedCh := inner.Aborted()
	decidedCh := inner.Decided()

	for (abortedCh != nil) || (decidedCh != nil) {
		select {
		case <-n.ctx.Done():
			return
		case aborted, ok := <-abortedCh:
			if !ok {
				abortedCh = nil
				continue
			}
			select {
			case <-n.ctx.Done():
			case n.aborted <- abortedEnvelope{state: aborted}:
			}
		case decided, ok := <-decidedCh:
			if !ok {
				decidedCh = nil
				continue
			}
			env := decideEnvelope{
				val: decided,
				ts:  inner.Epoch(),
			}
			select {
			case <-n.ctx.Done():
			case n.decidedEnvsCh <- env:
			}
		}
	}
}

func (n *Node) Deliver(message types.Message) {
	select {
	case <-n.ctx.Done():
	case n.msgsCh <- message:
	}
}

func (n *Node) deliver(message types.Message) {
	switch msg := message.(type) {
	case StateMsg:
		n.deliverOrPrepare(msg.Epoch, msg)
	case AcceptMsg:
		n.deliverOrPrepare(msg.Epoch, msg)
	case ReadMsg:
		n.deliverOrPrepare(msg.Epoch, msg)
	case WriteMsg:
		n.deliverOrPrepare(msg.Epoch, msg)
	case DecidedMsg:
		n.deliverOrPrepare(msg.Epoch, msg)
	}
}

func (n *Node) deliverOrPrepare(epoch int, msg types.Message) {
	if n.inner == nil {
		n.inbox.push(epoch, msg)
		return
	}
	n.inner.Deliver(msg)
}

type msgsInbox struct {
	msgs map[int]map[uuid.UUID]types.Message
}

func newMsgsInbox() *msgsInbox {
	return &msgsInbox{
		msgs: make(map[int]map[uuid.UUID]types.Message),
	}
}

func (i *msgsInbox) push(epoch int, msg types.Message) {
	_, ok := i.msgs[epoch]
	if !ok {
		i.msgs[epoch] = make(map[uuid.UUID]types.Message)
	}
	_, ok = i.msgs[epoch][msg.ID()]
	if ok {
		return
	}
	i.msgs[epoch][msg.ID()] = msg
}

func (i *msgsInbox) buffered(epoch int) []types.Message {
	epochMsgs, ok := i.msgs[epoch]
	if !ok {
		return nil
	}

	msgs := make([]types.Message, 0, len(epochMsgs))
	for _, msg := range epochMsgs {
		msgs = append(msgs, msg)
	}
	return msgs
}

func (i *msgsInbox) bufferedAllEpoch() []types.Message {
	epoches := utils.KeysSlice(i.msgs)
	if epoches == nil {
		return nil
	}

	slices.Sort(epoches)

	msgs := make([]types.Message, 0, len(epoches))
	for _, epoch := range epoches {
		msgs = append(msgs, i.buffered(epoch)...)
	}
	return msgs
}

func (i *msgsInbox) clear(epoch int) {
	delete(i.msgs, epoch)
}
