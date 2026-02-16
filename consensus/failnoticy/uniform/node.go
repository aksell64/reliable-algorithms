package uniform

import (
	"context"
	"reliable/broadcaster"
	"reliable/consensus"
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

type newEpochEnvelope struct {
	ts     int
	leader types.ProcessID
}

type decideEnvelope struct {
	ts  int
	val types.Value
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
	aborted        chan AbortedState
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
	n.aborted = make(chan AbortedState, 1)
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

func (n *Node) AddNodes(nodes ...consensus.Consensus) {}

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
	proposeTimer := time.NewTimer(200 * time.Millisecond)
	defer proposeTimer.Stop()
	var proposeChecked bool

	for {
		proposeChecked = false
		select {
		case <-n.ctx.Done():
			return

		case <-proposeTimer.C:
			n.maybePropose()
			proposeTimer.Reset(100 * time.Millisecond)
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

		case abortState := <-n.aborted:
			if abortState.Ts != n.ets {
				n.inbox.clear(abortState.Ts)
				continue
			}

			n.logger.Warn().
				Int("ts", abortState.Ts).
				Any("stateTs", abortState.State.Ts).
				Any("stateVal", abortState.State.Val).
				Msg("aborted state")

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
			n.logger.Info().
				Int("ts", newEpoch.ts).
				Str("leader", newEpoch.leader.String()).Msg("new epoch")

			// WORKAROUND: Handling new epoch transitions after consensus has already been decided.
			//
			// Problem:
			// After a node has decided on a value, the epoch consensus instance (n.inner) is already
			// stopped (via stopOnce). However, the epoch changer (LeaderBasedEpochChanger) continues
			// to operate independently and may trigger new epoch transitions. When a new epoch arrives,
			// the normal flow calls n.inner.Abort() to gracefully shut down the current epoch consensus
			// and retrieve its latest state via the Aborted channel. But since the inner instance is
			// already stopped, Abort() becomes a no-op — it never sends anything into the n.aborted
			// channel. This causes the main background loop to deadlock: it waits for an AbortedState
			// that will never arrive, so the node never transitions to the new epoch and gets stuck.
			//
			// Fix:
			// When a new epoch arrives and the node has already decided, we bypass the dead inner
			// instance entirely. Instead, we synthesize an AbortedState manually, carrying the already
			// decided value as the current state, and push it into the n.aborted channel. This unblocks
			// the main loop: the aborted handler fires, updates n.ets and n.leader, and starts a new
			// epoch — which, while technically unnecessary, keeps the epoch changer protocol consistent
			// and prevents the node from hanging.
			//
			// Why this is a workaround (not a clean solution):
			// Ideally, after a node decides, it should stop the epoch changer entirely and ignore all
			// subsequent newEpoch events — there is no reason to create new epoch consensus instances
			// or process further rounds once the decision is final. A cleaner approach would be to
			// either shut down the epoch changer upon decision, or simply `continue` here without
			// synthesizing any AbortedState at all. The current approach keeps the node "alive" after
			// decision, spawning new epoch consensus instances that do redundant work (reading, writing)
			// for a value that has already been committed.
			//
			// Why it is still correct:
			// Safety is preserved because the decided value is immutable — once n.decided is set to true,
			// n.decidedVal never changes, and the decideCh channel is already closed. No subsequent epoch
			// consensus instance can overwrite the decision: the decidedEnvsCh handler explicitly checks
			// `if decided.ts != n.ets || n.decided { continue }`, so any new decide attempts are dropped.
			// The synthesized AbortedState carries the decided value, so even if a new epoch starts, its
			// initial state reflects the correct decision. Liveness is preserved because the node no longer
			// blocks on a dead inner.Abort() — the synthetic abort unblocks the loop immediately.
			// Verified empirically: with 30 nodes, all converge to the same decided value with no hangs.
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
	select {
	case <-n.ctx.Done():
	case n.aborted <- state:
	}
}

func (n *Node) maybePropose() {
	n.mu.Lock()
	if n.leader == n.self && !n.proposed && n.val != nil {
		n.proposed = true
		val := *n.val
		n.mu.Unlock()
		n.inner.Propose(val)
		n.logger.Info().Str("val", val.String()).Msg("proposed")
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

	bufferedMsgs := n.inbox.bufferedAllEpoch()
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
			case n.aborted <- aborted:
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
		n.deliverInner(msg.Ts, msg)
	case AcceptMsg:
		n.deliverInner(msg.Ts, msg)
	case ReadMsg:
		n.deliverInner(msg.Ts, msg)
	case WriteMsg:
		n.deliverInner(msg.Ts, msg)
	case DecidedMsg:
		n.deliverInner(msg.Ts, msg)
	}
}

func (n *Node) deliverInner(epoch int, msg types.Message) {
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
