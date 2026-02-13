package uniform

import (
	"context"
	"reliable/broadcaster"
	"reliable/consensus"
	"reliable/logger"
	"reliable/p2p"
	"reliable/types"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

type newEpochEnvelope struct {
	ts     int
	leader types.ProcessID
}

type proposeEnvelope struct {
	ets int
	val types.Value
}

type Node struct {
	types.Deliverer
	ctx            context.Context
	cancel         context.CancelFunc
	self           types.ProcessID
	processesCount int
	inner          *epochConsensus
	epochChanger   *LeaderBasedEpochChanger
	newTs          int
	newLeader      types.ProcessID
	ets            int
	leader         types.ProcessID
	val            *types.Value
	proposed       bool
	decidedCh      chan types.Value
	aborted        chan AbortedState
	initEpoch      bool
	state          *State
	newEpochCh     chan newEpochEnvelope
	wg             sync.WaitGroup
	proposeCh      chan proposeEnvelope
	pl             p2p.Link
	beb            broadcaster.Broadcaster
	once           *types.WorkerOnce
	stopCh         chan struct{}
	logger         zerolog.Logger
}

func New(
	ctx context.Context,
	self types.ProcessID,
	changer *LeaderBasedEpochChanger,
	pl p2p.Link,
	beb broadcaster.Broadcaster,
) *Node {
	n := new(Node)
	n.ctx, n.cancel = context.WithCancel(ctx)
	n.self = self
	n.Deliverer = types.NewUnaryDeliverer(self)
	n.pl = pl
	n.pl.AddDeliverer(n)
	n.beb = beb
	n.beb.AddDeliverer(n)
	n.epochChanger = changer
	n.newEpochCh = make(chan newEpochEnvelope, 50)
	n.proposeCh = make(chan proposeEnvelope, 50)
	n.decidedCh = make(chan types.Value, 1)
	n.aborted = make(chan AbortedState, 1)
	n.stopCh = make(chan struct{})
	n.once = types.NewWorkerOnce()
	n.logger = logger.NewNodeScopeLogger(self, logger.Scope{"consensus", "uni"})
	return n
}

func (n *Node) Init() {
	n.once.Init(func() {
		n.epochChanger.Init()
		n.pl.Init()
		n.beb.Init()
		n.val = nil
		n.proposed = false
		n.initEpoch = true
		n.ets = 0
	})
}

func (n *Node) Start() {
	n.once.Stop(func() {
		n.epochChanger.SetEpochStarter(n)
		n.beb.Start()
		n.pl.Start()
		n.epochChanger.Start()
		go n.background()
	})
}

func (n *Node) Stop() {
	n.stopOnce()
}

func (n *Node) AddNodes(nodes ...consensus.Consensus) {
	n.processesCount += len(nodes)
}

func (n *Node) Propose(v types.Value) {
	vcp := v.Copy()
	n.val = &vcp
}

func (n *Node) Decided() <-chan types.Value {
	return n.decidedCh
}

func (n *Node) Crashed() chan struct{} {
	return n.stopCh
}

func (n *Node) StartEpoch(ts int, leader types.ProcessID) {
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
	proposeTimer := time.NewTimer(100 * time.Millisecond)
	defer proposeTimer.Stop()

	defer n.stopOnce()

	for {
		select {
		case <-n.ctx.Done():
			return

		case <-proposeTimer.C:
			n.maybePropose()
			proposeTimer.Reset(time.Second)

		case decided := <-n.decidedCh:
			n.decidedCh <- decided
			close(n.decidedCh)
			return

		case abortState := <-n.aborted:
			if abortState.Ts != n.ets {
				continue
			}
			n.logger.Warn().
				Int("ts", abortState.Ts).
				Any("stateTs", abortState.State.ts).
				Any("stateVal", abortState.State.val).
				Msg("aborted state")

			n.state = abortState.State
			n.ets = n.newTs
			n.leader = n.newLeader
			n.proposed = false
			n.startEpoch()

		case newEpoch := <-n.newEpochCh:
			//n.logger.Info().
			//	Int("newts", newEpoch.ts).
			//	Str("newLeader", newEpoch.leader.String()).
			//	Msg("new epoch")

			n.newTs, n.newLeader = newEpoch.ts, newEpoch.leader
			if !n.initEpoch {
				n.inner.Abort()
				continue
			}
			n.initEpoch = false
			n.startEpoch()

		case propose := <-n.proposeCh:
			if propose.ets != n.ets || n.proposed {
				continue
			}
			n.proposed = true
			n.inner.Propose(propose.val)
		}

		n.maybePropose()
	}
}

func (n *Node) stopOnce() {
	n.once.Stop(func() {
		n.cancel()
		n.wg.Wait()
		close(n.stopCh)
		n.pl.RemoveDeliverer(n)
		n.beb.RemoveDeliverer(n)
	})
}

func (n *Node) startEpoch() {
	node := newEpochConsensus(n.self, n.beb, n.pl, n.processesCount)
	node.StartEpoch(n.ctx, n.leader, n.ets, n.state)

	n.wg.Add(1)
	go n.waitFinishEpoch(node)
	n.inner = node
}

func (n *Node) waitFinishEpoch(inner *epochConsensus) {
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

			select {
			case <-n.ctx.Done():
			case n.decidedCh <- decided:
			}
		}
	}
}

func (n *Node) maybePropose() {
	canPropose := n.leader == n.self && !n.proposed && n.val != nil
	if !canPropose {
		return
	}

	env := proposeEnvelope{
		val: *n.val,
		ets: n.ets,
	}
	select {
	case <-n.ctx.Done():
	case n.proposeCh <- env:
	default:
	}
}
