package randomized

import (
	"context"
	"fmt"
	"reliable/broadcaster"
	"reliable/consensus/coin"
	"reliable/logger"
	"reliable/messages"
	"reliable/types"
	"reliable/utils"
	"reliable/utils/codec"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

const (
	phaseInit = "phaseInit"
	phase0    = "phase0"
	phase1    = "phase1"
	phase2    = "phase2"
)

// Node is Randomized Consensus with Large Domain instance
type Node struct {
	types.Deliverer
	ctx                  context.Context
	cancel               context.CancelFunc
	self                 types.ProcessID
	processes            map[types.ProcessID]struct{}
	round                int
	phase                string
	proposal             *types.Value
	vals                 map[types.ProcessID]*types.Value
	coinDomain           map[types.Value]struct{}
	decision             *types.Value
	quorum               int
	processesCount       int
	crashFaults          int
	decided              chan types.Value
	coin                 coin.TsCoinScheme
	pendingPhase         []phaseEvt
	drainPendingInterval time.Duration
	beb                  broadcaster.Broadcaster
	rb                   broadcaster.Broadcaster
	once                 *types.WorkerOnce
	decidedOnce          sync.Once
	stopCh               chan struct{}
	evts                 chan event
	logger               zerolog.Logger
	registry             *codec.Registry
}

func New(
	ctx context.Context,
	processes []types.ProcessID,
	self types.ProcessID,
	crashFaults int,
	coin coin.TsCoinScheme,
	beb broadcaster.Broadcaster,
	rb broadcaster.Broadcaster,
	log *zerolog.Logger,
	registry *codec.Registry,
) *Node {
	n := new(Node)
	n.ctx, n.cancel = context.WithCancel(ctx)
	n.Deliverer = types.NewUnaryDeliverer(self)
	n.self = self
	n.processes = make(map[types.ProcessID]struct{})
	for _, pid := range processes {
		n.processes[pid] = struct{}{}
	}
	n.processesCount = len(n.processes)
	n.quorum = n.processesCount/2 + 1
	n.crashFaults = crashFaults
	n.coin = coin
	n.coinDomain = make(map[types.Value]struct{})

	n.beb = beb
	beb.AddDeliverer(n, types.DelivererWithMsgNames(PhaseMsgName))
	n.rb = rb
	rb.AddDeliverer(n, types.DelivererWithMsgNames(ProposalMsgName, DecidedMsgName))

	n.once = types.NewWorkerOnce()
	n.evts = make(chan event, n.processesCount)
	n.decided = make(chan types.Value, 1)
	n.pendingPhase = make([]phaseEvt, 0)
	n.stopCh = make(chan struct{})
	n.drainPendingInterval = 300 * time.Millisecond
	if log != nil {
		n.logger = logger.NewNodeScopeLoggerFrom(*log, logger.Scope{"consensus", "randomized"})
	} else {
		n.logger = logger.NewNodeScopeLogger(self, logger.Scope{"consensus", "randomized"})
	}

	n.registry = registry

	n.registerCodec()

	return n
}

func (n *Node) Init() {
	n.once.Init(func() {
		types.InitWorkers(n.rb, n.beb)
		n.round = 0
		n.phase = phaseInit
		n.vals = make(map[types.ProcessID]*types.Value)
		n.coin.SetReceiver(n)
	})
}

func (n *Node) Start() {
	n.once.Start(func() {
		types.StartWorkers(n.rb, n.beb)
		go n.background()
	})
}

func (n *Node) Stop() {
	n.once.Stop(func() {
		n.rb.RemoveDeliverer(n)
		n.beb.RemoveDeliverer(n)
		types.StopWorkers(n.rb, n.beb)
		n.stop()
	})
}

func (n *Node) stop() {
	n.cancel()
	<-n.stopCh
	close(n.decided)
	close(n.evts)
}

func (n *Node) Propose(v types.Value) {
	n.triggerApply(proposeEvt{v})
	n.logger.Info().
		Str("val", v.String()).
		Msg("proposed")
}

func (n *Node) Decided() <-chan types.Value {
	return n.decided
}

func (n *Node) Crashed() chan struct{} {
	return nil
}

func (n *Node) background() {
	defer close(n.stopCh)
	drainPendingPhaseTimer := time.NewTimer(n.drainPendingInterval)
	defer drainPendingPhaseTimer.Stop()
	cooldownInterval := 50 * time.Millisecond

	for n.ctx.Err() == nil {
		select {
		case <-n.ctx.Done():
			return
		case <-drainPendingPhaseTimer.C:
			if n.phase != phaseInit {
				n.drainPendingPhase()
			}
			drainPendingPhaseTimer.Reset(n.drainPendingInterval)
		case evt := <-n.evts:
			err := n.handleEvt(evt)
			if err != nil {
				n.logger.Error().Err(err).Msg("handling event")
				continue
			}
			select {
			case <-n.ctx.Done():
			case <-time.After(cooldownInterval):
			}
		}
	}
}

func (n *Node) handleEvt(evt event) error {
	var err error
	switch e := evt.(type) {
	case proposalEvt:
		n.handleProposal(e)
	case proposeEvt:
		err = n.handlePropose(e)
	case phaseEvt:
		_, err = n.handlePhase(e)
	case coinEvt:
		err = n.handleCoin(e)
	case decidedEvt:
		n.handleDecided(e)
	}

	return err
}

func (n *Node) handleProposal(evt proposalEvt) {
	n.coinDomain[evt.val] = struct{}{}
}

func (n *Node) handlePropose(evt proposeEvt) error {
	n.proposal = &evt.val
	n.round = 1
	n.phase = phase1

	phaseMsg, err := n.makePhaseMsg()
	if err != nil {
		return fmt.Errorf("make phase msg: %w", err)
	}

	proposalMsg, err := n.makeProposalMsg(evt.val)
	if err != nil {
		return fmt.Errorf("make proposal msg: %w", err)
	}

	n.beb.Broadcast(n.ctx, phaseMsg)
	n.rb.Broadcast(n.ctx, proposalMsg)

	return nil
}

func (n *Node) handlePhase(evt phaseEvt) (bool, error) {
	if n.dropOrBuffered(evt) {
		return false, nil
	}

	var err error
	switch evt.phase {
	case phase1:
		err = n.handlePhase1(evt.from, evt.proposal)
	case phase2:
		n.handlePhase2(evt.from, evt.proposal)
	}

	if err != nil {
		return true, nil
	}

	return true, nil
}

func (n *Node) dropOrBuffered(evt phaseEvt) bool {
	if n.phase == phaseInit {
		n.bufferedPhase(evt)
		return true
	}

	if evt.round > n.round {
		n.bufferedPhase(evt)
		return true
	}

	if evt.round < n.round {
		n.dropPhase(evt)
		return true
	}

	if evt.round == n.round {
		if n.phase != phase0 && n.phaseOrd(evt.phase) > n.phaseOrd(n.phase) {
			n.bufferedPhase(evt)
			return true
		}
		if n.phase != phase0 && n.phaseOrd(evt.phase) < n.phaseOrd(n.phase) {
			n.dropPhase(evt)
			return true
		}

		if n.phase == phase0 && evt.phase == phase2 {
			n.vals[evt.from] = evt.proposal
			n.dropPhase(evt)
			return true
		}
	}

	return false
}

func (n *Node) dropPhase(evt phaseEvt) {
	n.logger.Warn().
		Str("from", evt.from.String()).
		Str("evtPhase", evt.phase).
		Int("evtRound", evt.round).
		Any("evtProposal", evt.proposal).
		Str("myPhase", n.phase).
		Int("myRound", n.round).
		Msg("dropping stale phase")
}

func (n *Node) bufferedPhase(evt phaseEvt) {
	n.logger.Info().
		Str("from", evt.from.String()).
		Str("evtPhase", evt.phase).
		Int("evtRound", evt.round).
		Any("evtProposal", evt.proposal).
		Str("myPhase", n.phase).
		Int("myRound", n.round).
		Msg("buffering phase")

	n.pendingPhase = append(n.pendingPhase, evt)
}

func (n *Node) handlePhase1(from types.ProcessID, val *types.Value) error {
	n.vals[from] = val

	n.logger.Info().
		Str("from", from.String()).
		Any("val", val).
		Str("phase", n.phase).
		Int("round", n.round).
		Any("vals", n.vals).
		Int("need", n.quorum).
		Int("receivedCount", len(n.vals)).
		Msg("received")

	if n.decision != nil {
		return nil
	}
	if len(n.vals) < n.quorum {
		return nil
	}

	majorityValue := n.findMajorityValue(n.quorum)
	n.proposal = nil
	if majorityValue != nil {
		n.proposal = majorityValue
	}

	n.logger.Info().
		Str("from", from.String()).
		Str("phase", n.phase).
		Int("round", n.round).
		Any("majority", n.proposal).
		Any("vals", n.vals).
		Msg("phase1->phase2")

	n.vals = make(map[types.ProcessID]*types.Value)
	n.phase = phase2

	msg, err := n.makePhaseMsg()
	if err != nil {
		return fmt.Errorf("make phase msg: %w", err)
	}

	n.beb.Broadcast(n.ctx, msg)
	return nil
}

func (n *Node) handlePhase2(from types.ProcessID, val *types.Value) {
	n.vals[from] = val
	n.logger.Info().
		Str("from", from.String()).
		Any("val", val).
		Int("countReceived", len(n.vals)).
		Int("need", n.processesCount-n.crashFaults).
		Str("phase", n.phase).
		Int("round", n.round).
		Any("vals", n.vals).
		Msg("received")

	if n.decision != nil {
		return
	}
	if len(n.vals) < n.processesCount-n.crashFaults {
		return
	}

	n.logger.Info().
		Str("from", from.String()).
		Str("phase", n.phase).
		Int("round", n.round).
		Any("vals", n.vals).
		Msg("phase2->phase0")

	n.phase = phase0
	n.startCoinFlip()
}

func (n *Node) phaseOrd(p string) int {
	switch p {
	case phase0:
		return 0
	case phase1:
		return 1
	case phase2:
		return 2
	}
	return -1
}

func (n *Node) drainPendingPhase() {
	pending := n.pendingPhase[:]
	n.pendingPhase = n.pendingPhase[:0]
	countUnprocesses := 0
	for _, evt := range pending {
		processes, err := n.handlePhase(evt)
		if err != nil {
			n.logger.Error().Err(err).Msg("drain phase")
			continue
		}
		if !processes {
			countUnprocesses++
		}
	}

	if countUnprocesses > 0 {
		n.logger.Info().Int("unprocesses", countUnprocesses).Msg("drain pending")
	}
}

func (n *Node) handleCoin(evt coinEvt) error {
	if evt.round != n.round {
		return nil
	}

	majorityValue := n.findMajorityValue(n.crashFaults)
	if majorityValue != nil {
		n.decision = majorityValue
		n.logger.Info().
			Any("val", n.decision).
			Str("phase", n.phase).
			Int("round", n.round).
			Any("vals", n.vals).
			Any("coin", evt.output).
			Msg("majority decision")

		msg, err := n.makeDecidedMsg(*n.decision)
		if err != nil {
			return fmt.Errorf("make decide msg: %w", err)
		}

		n.rb.Broadcast(n.ctx, msg)
		return nil
	}

	var existsProposal bool
	for _, v := range n.vals {
		if v != nil {
			n.proposal = v
			existsProposal = true
			break
		}
	}

	if !existsProposal {
		n.proposal = &evt.output
	}

	n.logger.Info().
		Str("phase", n.phase).
		Int("round", n.round).
		Any("vals", n.vals).
		Any("coin", evt.output).
		Msg("coin->phase1")

	n.vals = make(map[types.ProcessID]*types.Value)
	n.round++
	n.phase = phase1

	msg, err := n.makePhaseMsg()
	if err != nil {
		return fmt.Errorf("make phase msg: %w", err)
	}

	n.beb.Broadcast(n.ctx, msg)

	return nil
}

func (n *Node) handleDecided(evt decidedEvt) {
	n.decide(&evt.val)
}

func (n *Node) decide(v *types.Value) {
	n.decidedOnce.Do(func() {
		n.decision = v
		n.decided <- *n.decision
		close(n.decided)
	})
}

func (n *Node) findMajorityValue(threshold int) *types.Value {
	counts := make(map[types.Value]int)
	for _, v := range n.vals {
		if v == nil {
			continue
		}
		counts[*v]++
	}

	for val, count := range counts {
		if count >= threshold {
			result := val
			return &result
		}
	}
	return nil
}

func (n *Node) startCoinFlip() {
	coinDomain := n.sortedCoinDomain()
	n.logger.Info().
		Str("phase", n.phase).
		Int("round", n.round).
		Any("domain", coinDomain).
		Msg("start coin flip")

	go n.coin.RunScheme(n.round, coinDomain)
}

func (n *Node) sortedCoinDomain() []types.Value {
	domain := utils.KeysSlice(n.coinDomain)
	slices.SortFunc(domain, func(a, b types.Value) int {
		if a.Less(b) {
			return -1
		}
		return 1
	})
	return domain
}

func (n *Node) ReceiveCoinFlip(val types.Value, ts int) {
	n.triggerApply(coinEvt{output: val, round: ts})
}

func (n *Node) Deliver(msg types.Message) {
	switch m := msg.(type) {
	case ProposalMsg:
		n.deliverProposal(m)
	case PhaseMsg:
		n.deliverPhase(m)
	case DecidedMsg:
		n.deliverDecide(m)
	}
}

func (n *Node) deliverProposal(msg ProposalMsg) {
	valObj, err := n.registry.Unmarshal(msg.ValueRaw.Raw, msg.ValueRaw.RawType)
	if err != nil {
		n.logger.Error().Err(err).Msg("unmarshal proposal")
		return
	}

	val, ok := valObj.(types.Value)
	if !ok {
		n.logger.Error().Msg("cast proposal")
		return
	}

	n.asyncTriggerApply(proposalEvt{val: val})
}

func (n *Node) deliverPhase(msg PhaseMsg) {
	var value *types.Value

	if msg.ProposalRaw != nil {
		valObj, err := n.registry.Unmarshal(msg.ProposalRaw.Raw, msg.ProposalRaw.RawType)
		if err != nil {
			n.logger.Error().Err(err).Msg("unmarshal phase")
			return
		}

		val, ok := valObj.(types.Value)
		if !ok {
			n.logger.Error().Err(err).Msg("cast phase")
			return
		}
		value = &val
	}

	n.asyncTriggerApply(phaseEvt{
		msg.From(),
		msg.Phase,
		msg.Round,
		value,
	})
}

func (n *Node) deliverDecide(msg DecidedMsg) {
	valObj, err := n.registry.Unmarshal(msg.DecidedRaw.Raw, msg.DecidedRaw.RawType)
	if err != nil {
		n.logger.Error().Err(err).Msg("unmarshal decide")
		return
	}

	val, ok := valObj.(types.Value)
	if !ok {
		n.logger.Error().Err(err).Msg("cast decide")
		return
	}

	n.asyncTriggerApply(decidedEvt{
		val: val,
	})
}

func (n *Node) asyncTriggerApply(evt event) {
	go n.triggerApply(evt)
}

func (n *Node) triggerApply(evt event) {
	select {
	case <-n.ctx.Done():
	case n.evts <- evt:
	}
}

func (n *Node) makeProposalMsg(v types.Value) (types.Message, error) {
	raw, err := n.registry.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("marshal msg: %w", err)
	}

	msg := ProposalMsg{
		BaseMsg:  messages.NewBase(uuid.New(), n.self, ProposalMsgName),
		ValueRaw: messages.NewRaw(raw, v.Type()),
	}

	return msg, nil
}

func (n *Node) makePhaseMsg() (types.Message, error) {
	raw, err := n.registry.Marshal(n.proposal)
	if err != nil {
		return nil, fmt.Errorf("marshal msg: %w", err)
	}

	var proposal *messages.RawMsg
	if n.proposal != nil {
		val := *n.proposal
		valRaw := messages.NewRaw(raw, val.Type())
		proposal = &valRaw
	}

	msg := PhaseMsg{
		BaseMsg:     messages.NewBase(uuid.New(), n.self, PhaseMsgName),
		Phase:       n.phase,
		Round:       n.round,
		ProposalRaw: proposal,
	}

	return msg, nil
}

func (n *Node) makeDecidedMsg(val types.Value) (types.Message, error) {
	raw, err := n.registry.Marshal(val)
	if err != nil {
		return nil, fmt.Errorf("marshal msg: %w", err)
	}

	msg := DecidedMsg{
		BaseMsg:    messages.NewBase(uuid.New(), n.self, DecidedMsgName),
		DecidedRaw: messages.NewRaw(raw, utils.PtrValue(n.decision).Type()),
	}

	return msg, nil
}

func (n *Node) registerCodec() {
	codec.RegisterTyped[ProposalMsg](n.registry)
	codec.RegisterTyped[DecidedMsg](n.registry)
	codec.RegisterTyped[PhaseMsg](n.registry)
	types.RegisterIntValue(n.registry)
}
