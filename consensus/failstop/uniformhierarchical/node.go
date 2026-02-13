package uniformhierarchical

import (
	"context"
	"reliable/broadcaster"
	"reliable/consensus"
	"reliable/failure"
	"reliable/logger"
	"reliable/messages"
	"reliable/p2p"
	"reliable/types"
	"reliable/utils"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

type Config struct {
	ProposeInterval  time.Duration
	DecisionInterval time.Duration
	RoundInterval    time.Duration
	Runtime          *Runtime
}

type Node struct {
	types.Deliverer
	ctx              context.Context
	stop             context.CancelFunc
	self             types.ProcessID
	selfRank         types.ProcessRank
	processRanks     map[types.ProcessID]types.ProcessRank
	detectedRanks    map[types.ProcessRank]struct{}
	proposed         map[types.ProcessRank]types.Value
	ackRanks         map[types.ProcessRank]struct{}
	round            int
	proposal         *types.Value
	decision         types.Value
	decisionReady    bool
	decided          chan types.Value
	pl               p2p.Link
	rb               broadcaster.Broadcaster
	beb              broadcaster.Broadcaster
	pfd              *failure.PerfectFailureDetector
	proposeInterval  time.Duration
	decisionInterval time.Duration
	decisionSignals  chan struct{}
	roundInterval    time.Duration
	roundSignals     chan struct{}
	logger           zerolog.Logger
	crashed          chan struct{}

	once *types.WorkerOnce

	mu      sync.RWMutex
	runtime *Runtime
}

func New(
	ctx context.Context,
	self types.ProcessID,
	pl p2p.Link,
	rb broadcaster.Broadcaster,
	beb broadcaster.Broadcaster,
	pfd *failure.PerfectFailureDetector,
	cfg Config,
) *Node {
	node := new(Node)
	node.Deliverer = types.NewUnaryDeliverer(self)
	cctx, stop := context.WithCancel(ctx)
	node.ctx = cctx
	node.stop = stop
	node.self = self
	node.processRanks = make(map[types.ProcessID]types.ProcessRank)
	node.detectedRanks = make(map[types.ProcessRank]struct{})
	node.proposed = make(map[types.ProcessRank]types.Value)
	node.ackRanks = make(map[types.ProcessRank]struct{})
	node.decided = make(chan types.Value, 1)
	node.proposeInterval = cfg.ProposeInterval
	node.roundInterval = cfg.RoundInterval
	node.decisionInterval = cfg.DecisionInterval
	node.decisionSignals = make(chan struct{}, 1)
	node.roundSignals = make(chan struct{}, 1)
	node.crashed = make(chan struct{}, 1)
	node.logger = logger.NewNodeScopeLogger(self, logger.Scope{"consensus", "uniHierarchical"})
	node.once = types.NewWorkerOnce()

	pl.AddDeliverer(node)
	node.pl = pl
	rb.AddDeliverer(node)
	node.rb = rb
	beb.AddDeliverer(node)
	node.beb = beb
	pfd.AddDeliverer(node)
	node.pfd = pfd

	var r = cfg.Runtime
	if cfg.Runtime == nil {
		r = NewRuntime()
	}
	node.runtime = r

	return node
}

func (n *Node) AddNodes(nodes ...consensus.Consensus) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, node := range nodes {
		n.processRanks[node.ProcessID()] = types.ProcessRank(node.ProcessID())
		n.rb.AddCorrect(node.ProcessID())
		n.beb.AddCorrect(node.ProcessID())
	}
}

func (n *Node) Init() {
	n.once.Init(func() {
		n.pl.Init()
		n.beb.Init()
		n.rb.Init()
		n.pfd.Init()
		n.pfd.Subscribe(n)
		n.round = 1
		n.selfRank = types.ProcessRank(n.self)
		n.processRanks[n.self] = n.selfRank
	})
}

func (n *Node) Start() {
	n.once.Start(func() {
		n.pl.Start()
		n.beb.Start()
		n.rb.Start()
		n.pfd.Start()
		go n.background()
	})
}

func (n *Node) Stop() {
	n.once.Stop(func() {
		n.stop()
		n.pfd.Stop()
		n.beb.Stop()
		n.rb.Stop()
		n.pl.Stop()
		close(n.crashed)
		n.logger.Error().Msg("crashed")
	})
}

func (n *Node) Propose(v types.Value) {
	n.logger.Info().Str("value", v.String()).Msg("propose")
	newValue := v.Copy()

	n.mu.Lock()
	defer n.mu.Unlock()
	n.proposal = &newValue
}

func (n *Node) Decided() <-chan types.Value {
	return n.decided
}

func (n *Node) OnCrash(pid types.ProcessID) {
	n.mu.Lock()
	defer n.mu.Unlock()

	rank, ok := n.processRanks[pid]
	if !ok {
		return
	}
	if _, ok := n.detectedRanks[rank]; ok {
		return
	}

	n.beb.RemoveCorrect(pid)
	n.rb.RemoveCorrect(pid)

	//n.logger.Warn().Str("pid", pid.String()).Msg("crashed")
	n.detectedRanks[rank] = struct{}{}
	utils.Trigger(n.ctx, n.roundSignals)
}

func (n *Node) Deliver(msg types.Message) {
	switch m := msg.(type) {
	case AckMsg:
		n.handleAck(m.From())
	case DecidedMsg:
		n.handleDecided(m.From(), m.Decision)
	case ProposalMsg:
		n.handleProposal(m.From(), m.Proposal)
	}
}

func (n *Node) handleAck(from types.ProcessID) {
	n.mu.Lock()
	round := n.round
	rank, ok := n.processRanks[from]
	if !ok {
		return
	}
	n.ackRanks[rank] = struct{}{}
	ackCount := len(n.ackRanks)
	n.mu.Unlock()

	n.processEvent(HandleAckEvt{
		Round:       round,
		From:        from,
		ReceivedAck: ackCount,
		PID:         n.self,
	})
	//n.logger.Info().Str("from", from.String()).Any("acks", utils.KeysSlice(n.ackRanks)).Msg("handleAck")
}

func (n *Node) handleDecided(from types.ProcessID, decision types.Value) {
	n.mu.Lock()
	if n.decisionReady {
		n.mu.Unlock()
		return
	}
	n.decision = decision
	n.decisionReady = true
	n.mu.Unlock()

	n.logger.Info().Str("from", from.String()).Str("value", decision.String()).Msg("handleDecided")

	n.processEvent(ReadyDecideEvt{
		Decided: decision,
		PID:     n.self,
	})

	n.decide(decision)
}

func (n *Node) handleProposal(from types.ProcessID, proposal types.Value) {
	n.mu.Lock()
	rank, ok := n.processRanks[from]
	if !ok {
		n.mu.Unlock()
		return
	}
	n.proposed[rank] = proposal
	if rank.Int() < n.round {
		n.mu.Unlock()
		return
	}
	n.mu.Unlock()

	//n.logger.Info().Str("from", from.String()).Str("value", proposal.String()).Msg("handleProposal")
	msg := AckMsg{messages.NewBase(uuid.New(), n.self, AckMsgName)}
	n.pl.Send(from, msg)
}

func (n *Node) decide(v types.Value) {
	n.decided <- v
	close(n.decided)
	n.logger.Info().Str("value", v.String()).Msg("decided")
}

func (n *Node) background() {
	proposeTicker := time.NewTicker(n.proposeInterval)
	defer proposeTicker.Stop()
	roundTicker := time.NewTicker(n.roundInterval)
	defer roundTicker.Stop()
	decisionTicker := time.NewTicker(n.decisionInterval)
	defer decisionTicker.Stop()

	for {
		select {
		case <-n.ctx.Done():
			return
		case <-roundTicker.C:
			n.maybeNextRound()
		case <-n.roundSignals:
			n.maybeNextRound()
		case <-proposeTicker.C:
			n.maybePropose()
		case <-decisionTicker.C:
			n.maybeDecide()
		}
	}
}

func (n *Node) maybePropose() {
	n.mu.RLock()

	if n.proposal == nil {
		n.mu.RUnlock()
		return
	}

	proposal := *n.proposal

	if n.decisionReady || n.round != n.selfRank.Int() {
		n.mu.RUnlock()
		return
	}

	n.logger.Info().Str("proposal", proposal.String()).Msg("can propose")

	msg := ProposalMsg{
		Proposal: *n.proposal,
		Message:  messages.NewBase(uuid.New(), n.self, ProposalMsgName),
	}
	n.mu.RUnlock()

	n.beb.Broadcast(n.ctx, msg)
}

func (n *Node) maybeDecide() {
	n.mu.RLock()
	if n.proposal == nil {
		n.mu.RUnlock()
		return
	}
	allRanks := utils.Join(n.detectedRanks, n.ackRanks)
	NRanks := n.NRanks()

	//n.logger.Info().
	//	Any("all", utils.KeysSlice(allRanks)).
	//	Any("nranks", utils.KeysSlice(NRanks)).Msg("try decide")

	if !utils.IsEquals(allRanks, NRanks) {
		n.mu.RUnlock()
		return
	}
	msg := DecidedMsg{
		Decision: *n.proposal,
		Message:  messages.NewBase(uuid.New(), n.self, DecidedMsgName),
	}
	n.mu.RUnlock()

	n.logger.Info().Msg("can decide")

	n.rb.Broadcast(n.ctx, msg)
}

func (n *Node) maybeNextRound() {
	n.mu.Lock()
	defer n.mu.Unlock()

	if _, ok := n.detectedRanks[types.ProcessRank(n.round)]; !ok {
		return
	}
	currentProposed, ok := n.proposed[types.ProcessRank(n.round)]
	if ok {
		n.proposal = &currentProposed
	}
	n.round++
	n.logger.Info().Int("round", n.round).Msg("next round")
}

func (n *Node) NRanks() map[types.ProcessRank]struct{} {
	m := make(map[types.ProcessRank]struct{}, len(n.processRanks))
	for _, r := range n.processRanks {
		m[r] = struct{}{}
	}
	return m
}

func (n *Node) Instance() string {
	return "uc"
}

func (n *Node) Crashed() chan struct{} {
	return n.crashed
}

func (n *Node) processEvent(evt types.RuntimeEvent) {
	result := n.runtime.Upon(evt)
	if result.ShouldStop {
		n.Stop()
	}
}
