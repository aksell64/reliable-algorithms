package hierarchical

import (
	"context"
	"reliable/broadcaster"
	"reliable/consensus"
	"reliable/failure"
	"reliable/logger"
	"reliable/messages"
	"reliable/types"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
)

type Config struct {
	BroadcastTickerInterval time.Duration
	NextRoundTickerInterval time.Duration
}

type Node struct {
	types.Deliverer
	ctx            context.Context
	stop           context.CancelFunc
	pid            types.ProcessID
	selfRank       types.ProcessRank
	processesRanks map[types.ProcessID]types.ProcessRank
	detectedRanks  map[types.ProcessRank]struct{}
	round          int
	proposal       types.Value
	proposalReady  bool
	proposer       types.ProcessRank
	delivered      map[types.ProcessRank]bool
	broadcast      bool
	broadcaster    broadcaster.Broadcaster
	pfd            *failure.PerfectFailureDetector
	conf           Config
	active         atomic.Bool
	decided        chan types.Value
	mu             sync.RWMutex
	logger         zerolog.Logger
	once           *types.WorkerOnce
	crashed        chan struct{}
}

func New(
	ctx context.Context,
	pid types.ProcessID,
	rank types.ProcessRank,
	pfd *failure.PerfectFailureDetector,
	conf Config,
	beb broadcaster.Broadcaster,
) *Node {
	node := new(Node)
	cctx, stop := context.WithCancel(ctx)
	node.ctx = cctx
	node.stop = stop
	node.pid = pid
	node.selfRank = rank
	node.pfd = pfd
	node.processesRanks = make(map[types.ProcessID]types.ProcessRank)
	node.detectedRanks = make(map[types.ProcessRank]struct{})
	node.delivered = make(map[types.ProcessRank]bool)
	node.logger = logger.NewNodeLogger(node.pid)
	node.conf = conf
	node.decided = make(chan types.Value, 1)
	node.crashed = make(chan struct{})
	node.once = types.NewWorkerOnce()
	node.Deliverer = types.NewUnaryDeliverer(pid)

	node.broadcaster = beb
	beb.AddDeliverer(node)

	return node
}

func (n *Node) Init() {
	n.once.Init(func() {
		n.round = 1
		n.active.Store(true)

		n.broadcaster.Init()
		n.pfd.Init()
		n.pfd.Subscribe(n)
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
		n.stop()
		n.pfd.Stop()
		n.broadcaster.Stop()
		close(n.crashed)
	})
}

func (n *Node) AddNodes(nodes ...consensus.Consensus) {
	for _, node := range nodes {
		n.broadcaster.AddCorrect(node.ProcessID())
		rank := types.ProcessRank(node.ProcessID())
		n.processesRanks[node.ProcessID()] = rank
	}
}

func (n *Node) Decided() <-chan types.Value {
	return n.decided
}

func (n *Node) ProcessID() types.ProcessID {
	return n.pid
}

func (n *Node) OnCrash(pid types.ProcessID) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.pid == pid {
		n.active.Store(false)
		return
	}

	rank, ok := n.processesRanks[pid]
	if !ok {
		return
	}
	n.detectedRanks[rank] = struct{}{}
}

func (n *Node) HealthCheck() bool {
	return n.active.Load()
}

func (n *Node) Propose(v types.Value) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.proposalReady {
		n.proposal = v
		n.proposalReady = true
		n.logger.Info().
			Int("rank", n.selfRank.Int()).
			Str("value", v.String()).
			Msg("propose")
	}
}

func (n *Node) background() {
	broadcastTicker := time.NewTicker(n.conf.BroadcastTickerInterval)
	defer broadcastTicker.Stop()
	roundTicker := time.NewTicker(n.conf.NextRoundTickerInterval)
	defer roundTicker.Stop()

	for {
		select {
		case <-n.ctx.Done():
			return
		case <-broadcastTicker.C:
			if !n.canBroadcastDecide() {
				continue
			}

			n.logger.Info().Msg("can broadcast decided")
			n.mu.Lock()
			n.broadcast = true
			msg := messages.DecidedMessage{
				Decision: n.proposal,
				PID:      n.ProcessID(),
			}
			n.mu.Unlock()
			n.broadcaster.Broadcast(n.ctx, msg)
			n.decide(n.proposal)

		case <-roundTicker.C:
			if n.canNextRound() {
				n.mu.Lock()
				n.round++
				n.logger.Info().Int("round", n.round).Msg("next round")
				n.mu.Unlock()
			}
		}
	}
}

func (n *Node) canBroadcastDecide() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.selfRank.Int() == n.round && n.proposalReady && !n.broadcast
}

func (n *Node) canNextRound() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	_, isDetected := n.detectedRanks[types.ProcessRank(n.round)]
	isDelivered := n.delivered[types.ProcessRank(n.round)]

	return isDetected || isDelivered
}

func (n *Node) decide(v types.Value) {
	n.logger.Info().Str("value", v.String()).Msg("decide")
	n.decided <- v
	close(n.decided)
}

func (n *Node) Deliver(msg types.Message) {
	switch m := msg.(type) {
	case messages.DecidedMessage:
		n.handleDecided(m)
	}
}

func (n *Node) handleDecided(msg messages.DecidedMessage) {
	n.mu.Lock()
	defer n.mu.Unlock()

	rank, ok := n.processesRanks[msg.PID]
	if !ok {
		return
	}

	if rank < n.selfRank && rank > n.proposer {
		oldProposer := n.proposer
		oldProposal := n.proposal
		n.proposal = msg.Decision
		n.proposalReady = true
		n.proposer = rank
		n.logger.Info().
			Int("oldProposer", oldProposer.Int()).
			Str("oldProposal", oldProposal.String()).
			Int("newProposer", n.proposer.Int()).
			Str("newProposal", n.proposal.String()).
			Msg("decided")
	}

	n.delivered[rank] = true
}

func (n *Node) Instance() string {
	return "c"
}

func (n *Node) Crashed() chan struct{} {
	return n.crashed
}
