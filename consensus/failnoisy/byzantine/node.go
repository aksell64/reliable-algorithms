package byzantine

import (
	"context"
	"reliable/p2p"
	"reliable/types"
	"reliable/utils/codec"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

const (
	proposeInterval = time.Second
)

type epochInstance struct {
	epoch  int
	leader types.ProcessID
}

type decideEnvelope struct {
	val   types.Value
	epoch int
}

type abortedEnvelope struct {
	newEpoch  int
	newLeader types.ProcessID
}

type innerNode struct {
	node      *readWriteConsensus
	collector ConditionsCollector
}

type Node struct {
	types.Deliverer
	ctx           context.Context
	cancel        context.CancelFunc
	self          types.ProcessID
	processes     []types.ProcessID
	al            p2p.Link
	id            types.Identify
	val           *types.Value
	proposed      bool
	decided       bool
	currentEpoch  epochInstance
	newEpoch      epochInstance
	abortedStates chan AbortedState
	decidedCh     chan decideEnvelope
	decidedVal    types.Value
	decidedOut    chan types.Value
	faults        int
	abortCh       chan abortedEnvelope
	innerNodes    map[int]*innerNode
	registry      *codec.Registry
	logger        zerolog.Logger
	msgsCh        chan types.Message
	stopCh        chan struct{}
	wg            sync.WaitGroup
}

func (n *Node) Propose(v types.Value) {
	cv := v.Copy()
	n.val = &cv
}

func (n *Node) Decided() chan types.Value {
	return n.decidedOut
}

func (n *Node) Crashed() chan struct{} {
	return n.stopCh
}

func (n *Node) background() {
	proposeTimer := time.NewTimer(proposeInterval)
	defer proposeTimer.Stop()
	var proposeChecked bool
	for {
		proposeChecked = false
		select {
		case <-n.ctx.Done():
			return

		case env := <-n.abortCh:
			n.newEpoch.epoch = env.newEpoch
			n.newEpoch.leader = env.newLeader
			currentNode := n.getCurrentNode()
			if currentNode == nil {
				return
			}
			currentNode.Abort()

		case <-proposeTimer.C:
			n.maybePropose()
			proposeTimer.Reset(proposeInterval)
			proposeChecked = true

		case state := <-n.abortedStates:
			if state.Ts != n.currentEpoch.epoch {
				return
			}
			n.currentEpoch = n.newEpoch
			n.proposed = false
			inner := n.getInnerNode(state.Ts)
			if inner != nil {
				inner.collector.Stop()
			}
			n.startEpoch(*state.State)

		case decided := <-n.decidedCh:
			if decided.epoch != n.currentEpoch.epoch || n.decided {
				return
			}

			n.logger.Info().
				Str("val", decided.val.String()).
				Int("ts", decided.epoch).Msg("decided")

			n.decidedOut <- decided.val
			close(n.decidedOut)
			n.decided = true
			n.decidedVal = decided.val

		case msg := <-n.msgsCh:
			n.deliver(msg)
		}

		if !proposeChecked {
			n.maybePropose()
		}
	}
}

func (n *Node) StartEpoch(ts int, leader types.ProcessID) {
	env := abortedEnvelope{
		newEpoch:  ts,
		newLeader: leader,
	}

	select {
	case <-n.ctx.Done():
	case n.abortCh <- env:
	default:
	}
}

func (n *Node) startEpoch(state State) {
	collector := NewSignedConditionsCollector(
		n.ctx,
		n.self,
		n.id,
		n.processes,
		n.al,
		n.currentEpoch.leader,
		n.faults,
	)

	node := newEpochConsensus(
		n.ctx,
		n.al,
		n.self,
		n.processes,
		n.faults,
		0,
		n.registry,
		collector,
		&n.logger,
	)

	n.innerNodes[n.currentEpoch.epoch] = &innerNode{
		node:      node,
		collector: collector,
	}

	collector.Start()
	node.StartEpoch(n.ctx, n.currentEpoch.leader, n.currentEpoch.epoch, state)
	n.waitForEpoch(node)
}

func (n *Node) waitForEpoch(node *readWriteConsensus) {
	defer n.wg.Done()

	abortedCh := node.Aborted()
	decidedCh := node.Decided()

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
			case n.abortedStates <- aborted:
			}
		case decided, ok := <-decidedCh:
			if !ok {
				decidedCh = nil
				continue
			}
			env := decideEnvelope{
				val:   decided,
				epoch: node.Epoch(),
			}
			select {
			case <-n.ctx.Done():
			case n.decidedCh <- env:
			}
		}
	}
}

func (n *Node) maybePropose() {
	if n.currentEpoch.leader == n.self && !n.proposed && n.val != nil {
		n.proposed = true
		val := *n.val
		currentNode := n.getCurrentNode()
		if currentNode == nil {
			return
		}
		currentNode.Propose(val)
		n.logger.Info().Int("ets", n.currentEpoch.epoch).Str("val", val.String()).Msg("proposed")
		return
	}
}

func (n *Node) deliver(msg types.Message) {

}

func (n *Node) getCurrentNode() *readWriteConsensus {
	inner := n.getInnerNode(n.currentEpoch.epoch)
	if inner == nil {
		return nil
	}
	return inner.node
}

func (n *Node) getInnerNode(epoch int) *innerNode {
	return n.innerNodes[epoch]
}
