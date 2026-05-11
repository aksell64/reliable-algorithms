package byzantine

import (
	"context"
	"reliable/consensus"
	"reliable/logger"
	"reliable/p2p"
	"reliable/types"

	"github.com/rs/zerolog"
)

type LeaderSelector interface {
	CalcLeader(ts int) types.ProcessID
}

type newEpochEnvelope struct {
	from  types.ProcessID
	epoch int
}

type EpochChanger struct {
	types.Deliverer
	ctx          context.Context
	cancel       context.CancelFunc
	self         types.ProcessID
	al           p2p.Link
	processes    []types.ProcessID
	lastEpoch    int
	nextEpoch    int
	trusted      types.ProcessID
	newEpoches   map[types.ProcessID]struct{}
	faults       int
	selector     LeaderSelector
	starter      consensus.EpochStarter
	trustedCh    chan types.ProcessID
	newEpochesCh chan newEpochEnvelope
	stopCh       chan struct{}
	logger       zerolog.Logger
}

func NewEpochChanger(
	ctx context.Context,
	self types.ProcessID,
	al p2p.Link,
	processes []types.ProcessID,
	faults int,
	selector LeaderSelector,
) *EpochChanger {
	ec := new(EpochChanger)

	ec.ctx, ec.cancel = context.WithCancel(ctx)
	ec.self = self
	ec.al = al
	ec.Deliverer = types.NewUnaryDeliverer(self)
	ec.al.AddDeliverer(ec)
	ec.processes = processes
	ec.faults = faults
	ec.selector = selector
	ec.newEpoches = make(map[types.ProcessID]struct{})
	ec.newEpochesCh = make(chan newEpochEnvelope, 10)
	ec.trustedCh = make(chan types.ProcessID, 10)
	ec.stopCh = make(chan struct{})
	ec.logger = logger.NewNodeScopeLogger(self, logger.Scope{"epoch_changer", "byz"})

	ec.logger.Info().
		Int("processes", len(processes)).
		Int("faults", faults).
		Msg("created")

	return ec
}

func (ec *EpochChanger) SetStarter(starter consensus.EpochStarter) {
	ec.starter = starter
}

func (ec *EpochChanger) Init() {
	ec.lastEpoch = 1
	ec.nextEpoch = 1
	leader := ec.selector.CalcLeader(ec.lastEpoch)
	ec.triggerNewTrust(leader)
}

func (ec *EpochChanger) Start() {
	ec.logger.Info().Msg("start")
	go ec.background()
}

func (ec *EpochChanger) Stop() {
	ec.logger.Info().Msg("stopping")
	ec.cancel()
	<-ec.stopCh
	close(ec.newEpochesCh)
	close(ec.trustedCh)
	ec.logger.Info().Msg("stopped")
}

func (ec *EpochChanger) background() {
	defer close(ec.stopCh)
	for {
		select {
		case <-ec.ctx.Done():
			ec.logger.Info().Msg("context cancelled, background exiting")
			return
		case trusted := <-ec.trustedCh:
			ec.logger.Info().
				Str("trusted", trusted.String()).
				Str("prevTrusted", ec.trusted.String()).
				Msg("new trusted leader")
			ec.trusted = trusted
			ec.checkComplaint()
		case epochEnv := <-ec.newEpochesCh:
			if epochEnv.epoch != ec.lastEpoch+1 {
				ec.logger.Warn().
					Str("from", epochEnv.from.String()).
					Int("epoch", epochEnv.epoch).
					Int("expectedEpoch", ec.lastEpoch+1).
					Msg("NEWEPOCH ignored: unexpected epoch number")
				continue
			}
			ec.logger.Info().
				Str("from", epochEnv.from.String()).
				Int("epoch", epochEnv.epoch).
				Int("newEpochCount", len(ec.newEpoches)+1).
				Msg("NEWEPOCH received")
			ec.newEpoches[epochEnv.from] = struct{}{}
			ec.checkQuorums()
		}

		ec.checkComplaint()
		ec.checkQuorums()
	}
}

func (ec *EpochChanger) checkComplaint() {
	curLeader := ec.selector.CalcLeader(ec.lastEpoch)
	if ec.nextEpoch == ec.lastEpoch && ec.trusted != curLeader {
		ec.nextEpoch = ec.lastEpoch + 1
		ec.logger.Info().
			Str("trusted", ec.trusted.String()).
			Str("expectedLeader", curLeader.String()).
			Int("nextEpoch", ec.nextEpoch).
			Msg("complaint: trusted leader mismatch, broadcasting NEWEPOCH")
		ec.broadcastNewEpoch()
	}
}

func (ec *EpochChanger) checkQuorums() {
	count := len(ec.newEpoches)

	if count > ec.faults && ec.nextEpoch == ec.lastEpoch {
		ec.nextEpoch = ec.lastEpoch + 1
		ec.logger.Info().
			Int("count", count).
			Int("faults", ec.faults).
			Int("nextEpoch", ec.nextEpoch).
			Msg("f+1 quorum: joining epoch change, broadcasting NEWEPOCH")
		ec.broadcastNewEpoch()
	}

	if count > 2*ec.faults && ec.nextEpoch > ec.lastEpoch {
		ec.lastEpoch = ec.nextEpoch
		ec.newEpoches = make(map[types.ProcessID]struct{})
		leader := ec.selector.CalcLeader(ec.lastEpoch)
		ec.logger.Info().
			Int("count", count).
			Int("faults", ec.faults).
			Int("newEpoch", ec.lastEpoch).
			Str("newLeader", leader.String()).
			Msg("2f+1 quorum: starting new epoch")
		ec.starter.StartEpoch(ec.lastEpoch, leader)
	}
}

func (ec *EpochChanger) triggerNewTrust(trusted types.ProcessID) {
	select {
	case <-ec.ctx.Done():
	case ec.trustedCh <- trusted:
	}
}

func (ec *EpochChanger) triggerNewEpoch(from types.ProcessID, epoch int) {
	env := newEpochEnvelope{from: from, epoch: epoch}
	select {
	case <-ec.ctx.Done():
	case ec.newEpochesCh <- env:
	}
}

func (ec *EpochChanger) OnNewLeader(leader types.ProcessID) {
	ec.logger.Info().Str("leader", leader.String()).Msg("OnNewLeader from detector")
	ec.triggerNewTrust(leader)
}

func (ec *EpochChanger) handleNewEpoch(from types.ProcessID, epoch int) {
	if epoch != ec.lastEpoch+1 {
		return
	}
	ec.newEpoches[from] = struct{}{}
}

func (ec *EpochChanger) broadcastNewEpoch() {
	ec.logger.Info().
		Int("nextEpoch", ec.nextEpoch).
		Msg("broadcasting NEWEPOCH")
	msg := NewEpochMsg(ec.self, ec.nextEpoch)
	ec.alBroadcast(msg)
}

func (ec *EpochChanger) alBroadcast(msg types.Message) {
	for _, pid := range ec.processes {
		ec.al.Send(pid, msg)
	}
}

func (ec *EpochChanger) Deliver(msg types.Message) {
	emsg, ok := msg.(EpochMsg)
	if !ok {
		return
	}

	go ec.triggerNewEpoch(emsg.From(), emsg.Epoch)
}
