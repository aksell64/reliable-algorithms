package byzantine

import (
	"context"
	"reliable/logger"
	"reliable/p2p"
	"reliable/types"
	"reliable/types/inbox"
	"reliable/utils"
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

type LeaderDetector interface {
	Current() types.ProcessID
	OnComplain(id types.ProcessID)
}

type Node struct {
	types.Deliverer
	ctx            context.Context
	cancel         context.CancelFunc
	self           types.ProcessID
	processes      map[types.ProcessID]types.ProcessRank
	al             p2p.Link
	id             types.Identify
	val            *types.Value
	proposed       bool
	decided        bool
	currentEpoch   epochInstance
	newEpoch       epochInstance
	abortedStates  chan AbortedState
	decidedCh      chan decideEnvelope
	decidedVal     types.Value
	decidedOut     chan types.Value
	faults         int
	abortCh        chan abortedEnvelope
	innerNodes     map[int]*innerNode
	registry       *codec.Registry
	logger         zerolog.Logger
	msgsCh         chan types.Message
	stopCh         chan struct{}
	wg             sync.WaitGroup
	epochChanger   *EpochChanger
	inboxed        map[int]*tsMsgsInbox
	inbox          *inbox.Inbox
	validator      types.Validator
	leaderDetector LeaderDetector
}

func New(
	ctx context.Context,
	self types.ProcessID,
	processes map[types.ProcessID]types.ProcessRank,
	al p2p.Link,
	id types.Identify,
	changer *EpochChanger,
	faults int,
	registry *codec.Registry,
	inbox *inbox.Inbox,
	validator *types.Validator,
	leaderDetector LeaderDetector,
) *Node {
	n := new(Node)
	n.ctx, n.cancel = context.WithCancel(ctx)
	n.self = self
	n.processes = processes
	n.id = id
	n.faults = faults
	n.registry = registry
	n.Deliverer = types.NewUnaryDeliverer(self)
	n.al = al
	n.al.AddDeliverer(n)
	n.epochChanger = changer
	n.decidedOut = make(chan types.Value, 1)
	n.decidedCh = make(chan decideEnvelope, 1)
	n.abortedStates = make(chan AbortedState, 1)
	n.abortCh = make(chan abortedEnvelope, 1)
	n.msgsCh = make(chan types.Message, 100)
	n.stopCh = make(chan struct{})
	n.innerNodes = make(map[int]*innerNode)
	n.logger = logger.NewNodeScopeLogger(self, logger.Scope{"consensus", "byz"})
	n.logger.Info().
		Int("processes", len(processes)).
		Int("faults", faults).
		Msg("created")
	n.inbox = inbox
	n.inboxed = make(map[int]*tsMsgsInbox)
	n.validator = types.NoopValidator{}
	if validator != nil {
		n.validator = *validator
	}
	n.leaderDetector = leaderDetector

	return n
}

func (n *Node) Init() {
	n.logger.Info().Msg("init")
	n.al.Init()
	n.epochChanger.Init()
	n.currentEpoch.leader = n.leaderDetector.Current()
	n.currentEpoch.epoch = 1
}

func (n *Node) Start() {
	n.logger.Info().
		Int("epoch", n.currentEpoch.epoch).
		Str("leader", n.currentEpoch.leader.String()).
		Msg("start")
	n.al.Start()
	n.epochChanger.SetStarter(n)
	n.epochChanger.Start()
	n.wg.Go(n.background)

	n.startEpoch(State{Epoch: 1})
}

func (n *Node) Stop() {
	n.logger.Info().Msg("stopping")
	n.cancel()
	n.epochChanger.Stop()
	n.al.Stop()
	n.wg.Wait()
	n.logger.Info().Msg("stopped")
	for _, ibx := range n.inboxed {
		ibx.clear()
	}
}

func (n *Node) Propose(v types.Value) {
	n.logger.Info().Str("val", v.String()).Msg("propose queued")
	cv := v.Copy()
	n.val = &cv
}

func (n *Node) Decided() <-chan types.Value {
	return n.decidedOut
}

func (n *Node) Crashed() chan struct{} {
	return n.stopCh
}

func (n *Node) background() {
	defer close(n.stopCh)
	proposeTimer := time.NewTimer(proposeInterval)
	defer proposeTimer.Stop()

	var proposeChecked bool
	for {
		proposeChecked = false
		select {
		case <-n.ctx.Done():
			n.logger.Info().Msg("context cancelled, background exiting")
			return

		case env := <-n.abortCh:
			n.logger.Info().
				Int("newEpoch", env.newEpoch).
				Str("newLeader", env.newLeader.String()).
				Int("currentEpoch", n.currentEpoch.epoch).
				Msg("start epoch received, aborting current")
			n.newEpoch.epoch = env.newEpoch
			n.newEpoch.leader = env.newLeader
			currentNode := n.getCurrentNode()
			if currentNode == nil {
				n.logger.Warn().
					Int("epoch", n.currentEpoch.epoch).
					Msg("abort: no current inner node")
				continue
			}
			currentNode.Abort()

		case <-proposeTimer.C:
			n.maybePropose()
			proposeTimer.Reset(proposeInterval)
			proposeChecked = true

		case state := <-n.abortedStates:
			if state.Ts != n.currentEpoch.epoch {
				n.logger.Warn().
					Int("stateTs", state.Ts).
					Int("currentEpoch", n.currentEpoch.epoch).
					Msg("aborted: stale epoch, skipping")
				continue
			}

			n.logger.Info().
				Int("abortedEpoch", state.Ts).
				Int("nextEpoch", n.newEpoch.epoch).
				Str("nextLeader", n.newEpoch.leader.String()).
				Any("stateVal", state.State.Value).
				Msg("epoch aborted, starting new epoch")

			n.currentEpoch = n.newEpoch
			n.proposed = false
			inner := n.getInnerNode(state.Ts)
			if inner != nil {
				inner.collector.Stop()
			}
			n.startEpoch(*state.State)

		case decided := <-n.decidedCh:
			if decided.epoch != n.currentEpoch.epoch {
				n.logger.Warn().
					Int("decidedEpoch", decided.epoch).
					Int("currentEpoch", n.currentEpoch.epoch).
					Str("val", decided.val.String()).
					Msg("decided: stale epoch, ignoring")
				continue
			}
			if n.decided {
				n.logger.Warn().
					Str("val", decided.val.String()).
					Int("epoch", decided.epoch).
					Msg("decided: already decided, ignoring duplicate")
				continue
			}

			n.logger.Info().
				Str("val", decided.val.String()).
				Int("epoch", decided.epoch).
				Msg("decided")

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
	n.logger.Info().
		Int("ts", ts).
		Str("leader", leader.String()).
		Msg("StartEpoch triggered by epoch changer")
	env := abortedEnvelope{
		newEpoch:  ts,
		newLeader: leader,
	}

	select {
	case <-n.ctx.Done():
	case n.abortCh <- env:
	}
}

func (n *Node) startEpoch(state State) {
	n.logger.Info().
		Int("epoch", n.currentEpoch.epoch).
		Str("leader", n.currentEpoch.leader.String()).
		Any("stateVal", state.Value).
		Int("stateEpoch", state.Epoch).
		Msg("starting epoch consensus")

	processes := utils.KeysSlice(n.processes)

	collector := NewSignedConditionsCollector(
		n.ctx,
		n.self,
		n.id,
		processes,
		n.al,
		n.currentEpoch.leader,
		n.faults,
		n.currentEpoch.epoch,
	)

	node := newRWConsensus(
		n.ctx,
		n.al,
		n.self,
		processes,
		n.faults,
		1,
		n.registry,
		collector,
		&n.logger,
		n.validator,
		n.leaderDetector,
	)
	node.processesCount = len(n.processes)

	collector.SetReceiver(node)

	inner := &innerNode{
		node:      node,
		collector: collector,
	}

	n.innerNodes[n.currentEpoch.epoch] = inner

	collector.Start()
	node.StartEpoch(n.ctx, n.currentEpoch.leader, n.currentEpoch.epoch, state)

	defer func() {
		n.wg.Add(1)
		go n.waitForEpoch(node)
	}()

	inboxed, ok := n.inboxed[n.currentEpoch.epoch]
	if !ok {
		return
	}

	n.deliverBufferedMsgs(inboxed, collector, node)
}

func (n *Node) deliverBufferedMsgs(buffered *tsMsgsInbox, cc ConditionsCollector, rw *readWriteConsensus) {
	n.deliverCCMsgs(buffered, cc)
	n.deliverRwMsgs(buffered, rw)
}

func (n *Node) deliverCCMsgs(inboxed *tsMsgsInbox, collector ConditionsCollector) {
	msgs, err := inboxed.drainCCMsgs()
	if err != nil {
		n.logger.Error().Err(err).Msg("drain cc msgs")
		return
	}
	for _, msg := range msgs {
		collector.Deliver(msg)
	}
}

func (n *Node) deliverRwMsgs(inboxed *tsMsgsInbox, node *readWriteConsensus) {
	msgs, err := inboxed.drainRwMsgs()
	if err != nil {
		n.logger.Error().Err(err).Msg("drain rw msgs")
		return
	}
	for _, msg := range msgs {
		node.Deliver(msg)
	}
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
	if n.currentEpoch.leader != n.self {
		return
	}
	if n.proposed || n.val == nil {
		return
	}
	val := *n.val
	currentNode := n.getCurrentNode()
	if currentNode == nil {
		n.logger.Warn().
			Int("epoch", n.currentEpoch.epoch).
			Str("val", val.String()).
			Msg("maybePropose: no inner node, skipping")
		n.proposed = false
		return
	}
	currentNode.Propose(val)
	n.proposed = true
	n.logger.Info().
		Int("epoch", n.currentEpoch.epoch).
		Str("val", val.String()).
		Str("leader", n.currentEpoch.leader.String()).
		Msg("proposed to epoch consensus")
}

func (n *Node) deliver(msg types.Message) {
	processRw := func(msg types.Message, ts int) {
		inner := n.getInnerNode(ts)
		if inner == nil {
			ibx := n.getOrCreateTsInbox(ts)
			ibx.storeRWMsg(msg)
			return
		}
		inner.node.Deliver(msg)
	}

	processCC := func(msg types.Message, ts int) {
		inner := n.getInnerNode(ts)
		if inner == nil {
			ibx := n.getOrCreateTsInbox(ts)
			ibx.storeCCMsg(msg)
			return
		}
		inner.collector.Deliver(msg)
	}

	switch m := msg.(type) {
	case RWReadMsg:
		processRw(m, m.Ts)
	case RWStateMsg:
		processRw(m, m.Ts)
	case RWWriteMsg:
		processRw(m, m.Ts)
	case RWAcceptMsg:
		processRw(m, m.Ts)
	case CCSendMsg:
		processCC(m, m.Ts)
	case CCCollectedMsg:
		processCC(m, m.Ts)
	}
}

func (n *Node) getOrCreateTsInbox(ts int) *tsMsgsInbox {
	_, ok := n.inboxed[ts]
	if !ok {
		n.inboxed[ts] = &tsMsgsInbox{
			inner: n.inbox,
			ts:    ts,
		}
	}
	return n.inboxed[ts]
}

func (n *Node) Deliver(msg types.Message) {
	select {
	case <-n.ctx.Done():
	case n.msgsCh <- msg:
	}
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
