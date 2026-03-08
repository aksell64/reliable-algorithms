package byzantine

import (
	"context"
	"reliable/consensus"
	"reliable/election"
	"reliable/p2p"
	"reliable/types"
)

type LeaderSelector func(
	epoch int,
	processes []types.ProcessID) types.ProcessID

type newEpochEnvelope struct {
	from  types.ProcessID
	epoch int
}

type epochChanger struct {
	types.Deliverer
	ctx            context.Context
	cancel         context.CancelFunc
	self           types.ProcessID
	al             p2p.Link
	processes      []types.ProcessID
	leaderDetector *election.RotatingByzantineLeaderDetector
	lastEpoch      int
	nextEpoch      int
	trusted        types.ProcessID
	newEpoches     map[types.ProcessID]struct{}
	faults         int
	selector       LeaderSelector
	starter        consensus.EpochStarter
	trustedCh      chan types.ProcessID
	newEpochesCh   chan newEpochEnvelope
	stopCh         chan struct{}
}

func newEpochChanger(
	ctx context.Context,
	self types.ProcessID,
	al p2p.Link,
	processes []types.ProcessID,
	detector *election.RotatingByzantineLeaderDetector,
	faults int,
	selector LeaderSelector,
	starter consensus.EpochStarter,
) *epochChanger {
	ec := &epochChanger{}

	ec.ctx, ec.cancel = context.WithCancel(ctx)
	ec.self = self
	ec.al = al
	ec.Deliverer = types.NewUnaryDeliverer(self)
	ec.al.AddDeliverer(ec)
	ec.processes = processes
	ec.leaderDetector = detector
	ec.faults = faults
	ec.selector = selector
	ec.starter = starter
	ec.newEpoches = make(map[types.ProcessID]struct{})
	ec.newEpochesCh = make(chan newEpochEnvelope, 10)
	ec.trustedCh = make(chan types.ProcessID, 10)
	ec.stopCh = make(chan struct{})

	return ec
}

func (ec *epochChanger) Init() {
	leader := ec.selector(ec.lastEpoch, ec.processes)
	// non-blocking, buffered
	ec.triggerNewTrust(leader)
}

func (ec *epochChanger) Start() {
	go ec.background()
}

func (ec *epochChanger) Stop() {
	ec.cancel()
	<-ec.stopCh
	close(ec.newEpochesCh)
	close(ec.trustedCh)
}

func (ec *epochChanger) background() {
	defer close(ec.stopCh)
	for {
		select {
		case <-ec.ctx.Done():
			return
		case trusted := <-ec.trustedCh:
			ec.trusted = trusted
			ec.checkComplaint()
		case epochEnv := <-ec.newEpochesCh:
			if epochEnv.epoch != ec.lastEpoch+1 {
				continue
			}
			ec.newEpoches[epochEnv.from] = struct{}{}
			ec.checkQuorums()
		}

		ec.checkComplaint()
		ec.checkQuorums()
	}
}

func (ec *epochChanger) checkComplaint() {
	if ec.nextEpoch == ec.lastEpoch &&
		ec.trusted != ec.selector(ec.lastEpoch, ec.processes) {
		ec.nextEpoch = ec.lastEpoch + 1
		ec.broadcastNewEpoch()
	}
}

func (ec *epochChanger) checkQuorums() {
	count := len(ec.newEpoches)

	if count > ec.faults && ec.nextEpoch == ec.lastEpoch {
		ec.nextEpoch = ec.lastEpoch + 1
		ec.broadcastNewEpoch()
	}

	if count > 2*ec.faults && ec.nextEpoch > ec.lastEpoch {
		ec.lastEpoch = ec.nextEpoch
		ec.newEpoches = make(map[types.ProcessID]struct{})
		leader := ec.selector(ec.lastEpoch, ec.processes)
		ec.starter.StartEpoch(ec.lastEpoch, leader)
	}
}

func (ec *epochChanger) triggerNewTrust(trusted types.ProcessID) {
	select {
	case <-ec.ctx.Done():
	case ec.trustedCh <- trusted:
	}
}

func (ec *epochChanger) triggerNewEpoch(from types.ProcessID, epoch int) {
	env := newEpochEnvelope{from: from, epoch: epoch}
	select {
	case <-ec.ctx.Done():
	case ec.newEpochesCh <- env:
	}
}

func (ec *epochChanger) OnNewLeader(leader types.ProcessID) {
	ec.triggerNewTrust(leader)
}

func (ec *epochChanger) handleNewEpoch(from types.ProcessID, epoch int) {
	if epoch != ec.lastEpoch+1 {
		return
	}
	ec.newEpoches[from] = struct{}{}
}

func (ec *epochChanger) broadcastNewEpoch() {
	msg := NewEpochMsg(ec.self, ec.nextEpoch)
	ec.broadcast(msg)
}

func (ec *epochChanger) broadcast(msg types.Message) {
	for _, pid := range ec.processes {
		ec.al.Send(pid, msg)
	}
}

func (ec *epochChanger) Deliver(msg types.Message) {
	emsg, ok := msg.(EpochMsg)
	if !ok {
		return
	}

	go ec.triggerNewEpoch(emsg.From(), emsg.Epoch)
}
