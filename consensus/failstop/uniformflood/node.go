package uniformflood

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
	ctx            context.Context
	cancel         context.CancelFunc
	pid            types.ProcessID
	correct        map[types.ProcessID]types.Deliverer
	processesCount int
	round          int
	decision       types.Value
	decisionReady  bool
	receivedFrom   map[types.ProcessID]struct{}
	proposalSet    map[types.Value]struct{}
	selector       consensus.DeterministicSelector
	crashCh        chan types.ProcessID
	pfd            *failure.PerfectFailureDetector
	mu             sync.RWMutex
	active         atomic.Bool
	broadcaster    broadcaster.Broadcaster
	decided        chan types.Value
	logger         zerolog.Logger
	crashed        chan struct{}
	once           *types.WorkerOnce
}

func NewNode(
	ctx context.Context,
	pid types.ProcessID,
	selector consensus.DeterministicSelector,
	pfd *failure.PerfectFailureDetector,
	beb broadcaster.Broadcaster,
) *Node {
	node := new(Node)
	cctx, cancel := context.WithCancel(ctx)
	node.ctx = cctx
	node.cancel = cancel
	node.pid = pid
	node.pfd = pfd
	node.selector = selector
	node.decided = make(chan types.Value, 1)
	node.crashCh = make(chan types.ProcessID, 10)
	node.correct = make(map[types.ProcessID]types.Deliverer)
	node.proposalSet = make(map[types.Value]struct{})
	node.receivedFrom = make(map[types.ProcessID]struct{})
	node.crashed = make(chan struct{})
	node.Deliverer = types.NewUnaryDeliverer(pid)
	node.once = types.NewWorkerOnce()

	node.broadcaster = beb
	beb.AddDeliverer(node)

	node.logger = logger.NewNodeLogger(pid)
	return node
}

func (n *Node) AddNodes(nodes ...consensus.Consensus) {
	for _, node := range nodes {
		if n.pid == node.ProcessID() {
			return
		}
		n.correct[node.ProcessID()] = node
		n.processesCount++
		n.broadcaster.AddCorrect(node.ProcessID())
	}
}

func (n *Node) Init() {
	n.once.Init(func() {
		n.round = 1
		n.pfd.Subscribe(n)
		n.active.Store(true)
		n.broadcaster.Init()
		n.pfd.Init()
	})
}

func (n *Node) Start() {
	n.once.Start(func() {
		n.broadcaster.Start()
		n.pfd.Start()
		go n.background()
	})
}

func (n *Node) Stop() {
	n.once.Stop(func() {
		n.cancel()
		n.pfd.Stop()
		n.broadcaster.Stop()
		close(n.crashed)
	})
}

func (n *Node) OnCrash(pid types.ProcessID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.correct, pid)
}

func (n *Node) Propose(v types.Value) {
	n.logger.Info().Str("value", v.String()).Msg("Propose")

	n.mu.Lock()
	n.proposalSet[v] = struct{}{}
	n.mu.Unlock()

	n.broadcastProposal(1)
	n.handleProposal(n.pid, []types.Value{v}, 1)
}

func (n *Node) Decided() <-chan types.Value {
	return n.decided
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
			n.mu.RLock()
			if n.decisionReady {
				n.mu.RUnlock()
				return
			}

			if !utils.IsSubset(n.correct, n.receivedFrom) {
				n.mu.RUnlock()
				continue
			}

			if n.round == n.processesCount {
				decision := n.selector.Select(utils.KeysSlice(n.proposalSet))
				n.mu.RUnlock()
				n.decide(decision)
				continue
			}
			n.mu.RUnlock()

			n.nextRound()
		}
	}
}

func (n *Node) nextRound() {
	n.mu.Lock()
	n.round++
	n.receivedFrom = make(map[types.ProcessID]struct{})

	n.logger.Info().Int("round", n.round).Msg("next round")
	n.mu.Unlock()

	n.broadcastProposal(n.round)
}

func (n *Node) decide(val types.Value) {
	n.logger.Info().
		Str("value", val.String()).
		Msg("decide")

	n.mu.Lock()
	n.decision = val
	n.decisionReady = true
	n.mu.Unlock()
	n.decided <- val
	close(n.decided)

	n.logger.Info().
		Str("value", val.String()).
		Msg("Decide ready!")
}

func (n *Node) handleProposal(from types.ProcessID, proposals []types.Value, round int) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.round != round {
		return
	}

	n.receivedFrom[from] = struct{}{}
	for _, value := range proposals {
		n.proposalSet[value] = struct{}{}
	}

	n.logger.Info().
		Str("from", from.String()).
		Any("proposals", proposals).
		Any("received", utils.KeysSlice(n.receivedFrom)).
		Any("proposalSet", utils.KeysSlice(n.proposalSet)).
		Msg("handleProposal")
}

func (n *Node) broadcastProposal(round int) {
	n.mu.RLock()
	proposals := utils.KeysSlice(n.proposalSet)
	n.mu.RUnlock()

	msg := messages.ProposalMessage{
		Id:        uuid.New(),
		Proposals: proposals,
		PID:       n.pid,
		Round:     round,
	}

	n.broadcaster.Broadcast(n.ctx, msg)
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

func (n *Node) crushSelf() {
	n.active.Store(false)
}

func (n *Node) Deliver(msg types.Message) {
	n.deliver(msg)
}

func (n *Node) deliver(msg types.Message) {
	switch m := msg.(type) {
	case messages.ProposalMessage:
		n.handleProposal(m.PID, m.Proposals, m.Round)
	case messages.CrashMessage:
		n.handleCrash(m.PID)
	}
}

func (n *Node) ProcessID() types.ProcessID {
	return n.pid
}

func (n *Node) Instance() string {
	return "uc"
}

func (n *Node) Crashed() chan struct{} {
	return n.crashed
}
