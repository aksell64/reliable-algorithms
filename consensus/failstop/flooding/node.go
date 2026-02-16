package flooding

import (
	"context"
	"reliable/broadcaster"
	"reliable/consensus"
	"reliable/failure"
	"reliable/logger"
	"reliable/messages"
	"reliable/types"
	"reliable/utils"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

type Node struct {
	types.Deliverer
	ctx           context.Context
	stop          func()
	pid           types.ProcessID
	correct       map[types.ProcessID]types.Deliverer
	round         int
	decision      types.Value
	decisionReady bool
	receivedFrom  map[int]map[types.ProcessID]struct{}
	proposal      map[int]map[types.Value]struct{}
	selector      consensus.DeterministicSelector
	crashCh       chan types.ProcessID
	pfd           *failure.PerfectFailureDetector
	mu            sync.RWMutex
	pauser        utils.Pauser
	active        atomic.Bool
	broadcaster   broadcaster.Broadcaster
	decided       chan types.Value
	logger        zerolog.Logger
	crashed       chan struct{}
}

func New(
	ctx context.Context,
	pid types.ProcessID,
	selector consensus.DeterministicSelector,
	pfd *failure.PerfectFailureDetector,
	pauser utils.Pauser,
	bcast broadcaster.Broadcaster,
) *Node {
	node := new(Node)
	cctx, stop := context.WithCancel(ctx)
	node.ctx = cctx
	node.stop = stop
	node.pid = pid
	node.correct = make(map[types.ProcessID]types.Deliverer)
	node.round = 1
	node.decisionReady = false
	node.receivedFrom = make(map[int]map[types.ProcessID]struct{})
	node.proposal = make(map[int]map[types.Value]struct{})
	node.selector = selector
	node.logger = logger.NewNodeScopeLogger(pid, logger.Scope{"consensus", "flooding"})
	node.pauser = pauser
	node.crashCh = make(chan types.ProcessID, 10)
	node.pfd = pfd
	node.decided = make(chan types.Value, 1)
	node.Deliverer = types.NewUnaryDeliverer(pid)
	node.crashed = make(chan struct{})

	bcast.AddDeliverer(node)
	node.broadcaster = bcast

	return node
}

func (n *Node) Init() {
	correct := n.getCorrect()
	for _, pid := range correct {
		n.addReceivedRoundProcess(pid, 0)
	}
	n.addReceivedRoundProcess(n.pid, 0)

	n.logger.Info().Str("status", "success").Msg("init")

	correctIds := make([]types.ProcessID, 0)
	for _, correct := range n.correct {
		correctIds = append(correctIds, correct.ProcessID())
	}
	n.logger.Info().Any("correct", correctIds).Msg("init")

	n.active.Store(true)

	n.broadcaster.Init()
	n.pfd.Init()

	n.pfd.Subscribe(n)
}

func (n *Node) Start() {
	n.broadcaster.Start()
	n.pfd.Start()
	go n.background()
}

func (n *Node) AddNodes(nodes ...consensus.Consensus) {
	for _, node := range nodes {
		if n.pid == node.ProcessID() {
			continue
		}
		n.correct[node.ProcessID()] = node
		n.broadcaster.AddCorrect(node.ProcessID())
	}
}

func (n *Node) Decided() <-chan types.Value {
	return n.decided
}

func (n *Node) OnCrash(pid types.ProcessID) {
	select {
	case <-n.ctx.Done():
	case n.crashCh <- pid:
	default:
		go func() {
			select {
			case <-n.ctx.Done():
			case n.crashCh <- pid:
			}
		}()
	}
}

func (n *Node) handleCrash(pid types.ProcessID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.pid == pid {
		n.crushSelf()
		return
	}
	delete(n.correct, pid)
	n.logger.Error().Str("processID", pid.String()).Msg("crashed")
}

func (n *Node) Propose(v types.Value) {
	n.propose(v)
}

func (n *Node) propose(v types.Value) {
	n.logger.Info().Str("value", v.String()).Msg("propose")

	round := 1
	n.pauser.Checkpoint()
	n.handlePropose(n.pid, round, []types.Value{v})
	n.pauser.Checkpoint()
	msg := messages.ProposalMessage{
		Id:        uuid.New(),
		PID:       n.pid,
		Proposals: n.getProposals(round),
		Round:     round,
	}
	n.broadcaster.Broadcast(n.ctx, msg)
}

func (n *Node) handlePropose(pid types.ProcessID, round int, values []types.Value) {
	valuesStr := make([]string, 0, len(values))
	for _, v := range values {
		valuesStr = append(valuesStr, v.String())
	}

	n.logger.Info().
		Str("processID", pid.String()).
		Int("round", round).
		Any("values", valuesStr).Msg("handlePropose")

	n.mu.Lock()
	if n.decisionReady {
		n.logger.Info().
			Str("processID", pid.String()).
			Int("round", round).
			Any("values", valuesStr).Msg("handlePropose already decided")
		return
	}

	receiveCount := len(n.receivedFrom[round])
	valuesCount := len(n.proposal[round])

	n.pauser.Checkpoint()
	n.addReceivedRoundProcess(pid, round)
	for _, val := range values {
		n.addValueToRound(val, round)
	}

	newReceiveCount := len(n.receivedFrom[round])
	newValuesCount := len(n.proposal[round])

	receivedInRound := make([]string, 0)
	valsInRound := make([]string, 0)
	for value, _ := range n.proposal[n.round] {
		valsInRound = append(valsInRound, value.String())
	}
	for pid, _ := range n.receivedFrom[n.round] {
		receivedInRound = append(receivedInRound, pid.String())
	}

	n.mu.Unlock()

	n.logger.Info().
		Str("processID", pid.String()).
		Int("round", n.round).
		Int("delteRec", newReceiveCount-receiveCount).
		Int("deltaVal", newValuesCount-valuesCount).
		Any("received", receivedInRound).
		Any("vals", valsInRound).
		Msg("handlePropose result")
}

func (n *Node) background() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-n.ctx.Done():
			return
		case crashed := <-n.crashCh:
			if crashed == n.pid {
				return
			}
			n.handleCrash(crashed)
		case <-ticker.C:
			n.pauser.Checkpoint()
			n.mu.RLock()
			if n.decisionReady {
				return
			}
			receivedCurrent, ok := n.receivedFrom[n.round]
			if !ok {
				n.mu.RUnlock()
				continue
			}
			n.pauser.Checkpoint()
			correct := n.correctProcessesIDs()
			if !utils.IsSubset(correct, receivedCurrent) {
				n.mu.RUnlock()
				continue
			}
			n.pauser.Checkpoint()
			prevReceived := n.receivedFrom[n.round-1]
			canDecide := utils.IsEquals(receivedCurrent, prevReceived)
			n.mu.RUnlock()

			if !canDecide {
				n.pauser.Checkpoint()
				n.nextRound()
			} else {
				n.logger.Info().Msg("can decide")
				n.mu.RLock()
				decision := n.selector.Select(n.getProposals(n.round))
				n.mu.RUnlock()

				msg := messages.DecidedMessage{
					Id:       uuid.New(),
					Decision: decision,
					PID:      n.pid,
				}
				n.pauser.Checkpoint()
				n.broadcaster.Broadcast(n.ctx, msg)
				n.decide(decision)
			}
		}
	}
}

func (n *Node) nextRound() {
	n.pauser.Checkpoint()

	var round int
	n.mu.Lock()
	n.round = n.round + 1
	round = n.round
	n.mu.Unlock()

	n.pauser.Checkpoint()
	n.logger.Info().Int("round", round).Msg("nextRound")

	msg := messages.ProposalMessage{
		Id:        uuid.New(),
		Proposals: n.getProposals(n.round - 1),
		Round:     n.round,
		PID:       n.pid,
	}
	n.broadcaster.Broadcast(n.ctx, msg)
}

func (n *Node) decide(val types.Value) {
	n.pauser.Checkpoint()
	n.logger.Info().
		Str("value", val.String()).
		Msg("decide")

	n.mu.Lock()
	defer n.mu.Unlock()

	if n.decisionReady {
		return
	}

	n.decisionReady = true
	n.decision = val
	n.decided <- val
	close(n.decided)

	n.logger.Info().
		Str("value", val.String()).
		Msg("Decide ready!")
}

func (n *Node) handleDecide(pid types.ProcessID, value types.Value) {
	n.logger.Info().
		Str("from", pid.String()).
		Str("value", value.String()).
		Msg("handleDecide")

	n.mu.Lock()
	n.pauser.Checkpoint()
	_, isCorrect := n.correct[pid]
	if !isCorrect {
		return
	}
	n.mu.Unlock()

	n.decide(value)
}

func (n *Node) getCorrect() []types.ProcessID {
	processes := make([]types.ProcessID, 0, len(n.correct))
	for pid, _ := range n.correct {
		processes = append(processes, pid)
	}
	return processes
}

func (n *Node) getProposals(round int) []types.Value {
	valsMap, ok := n.proposal[round]
	if !ok {
		return []types.Value{}
	}
	vals := make([]types.Value, 0)
	for val, _ := range valsMap {
		vals = append(vals, val)
	}
	return vals
}

func (n *Node) addReceivedRoundProcess(pid types.ProcessID, round int) {
	roundPids, ok := n.receivedFrom[round]
	if !ok {
		roundPids = make(map[types.ProcessID]struct{})
		n.receivedFrom[round] = roundPids
	}

	n.receivedFrom[round][pid] = struct{}{}
}

func (n *Node) addValueToRound(v types.Value, round int) {
	roundVals, ok := n.proposal[round]
	if !ok {
		roundVals = make(map[types.Value]struct{})
		n.proposal[round] = roundVals
	}

	n.proposal[round][v] = struct{}{}
}

func (n *Node) correctProcessesIDs() map[types.ProcessID]struct{} {
	m := make(map[types.ProcessID]struct{})
	for _, n := range n.correct {
		m[n.ProcessID()] = struct{}{}
	}
	return m
}

func (n *Node) Deliver(msg types.Message) {
	n.pauser.Checkpoint()
	n.deliver(msg)
}

func (n *Node) deliver(msg types.Message) {
	switch m := msg.(type) {
	case messages.ProposalMessage:
		go n.handlePropose(m.PID, m.Round, m.Proposals)
	case messages.DecidedMessage:
		go n.handleDecide(m.PID, m.Decision)
	case messages.CrashMessage:
		go n.handleCrash(m.PID)
	}
}

func (n *Node) ProcessID() types.ProcessID {
	return n.pid
}

func (n *Node) crushSelf() {
	n.active.Store(false)
}

func (n *Node) HealthCheck() bool {
	return n.active.Load()
}

func (n *Node) Instance() string {
	return "c"
}

func (n *Node) Stop() {
	n.stop()
	n.pfd.Stop()
	n.broadcaster.Stop()
	close(n.crashed)
}

func (n *Node) Crashed() chan struct{} {
	return n.crashed
}
