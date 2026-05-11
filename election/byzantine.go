package election

import (
	"reliable/logger"
	"reliable/messages"
	"reliable/p2p"
	"reliable/types"
	"sync"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

type RotatingByzantineLeaderDetector struct {
	types.Deliverer
	self           types.ProcessID
	al             p2p.Link
	round          int
	complains      map[types.ProcessID]struct{}
	complained     bool
	faults         int
	processes      map[types.ProcessID]types.ProcessRank
	processesCount int
	receiver       Receiver
	mu             sync.RWMutex
	logger         zerolog.Logger
}

func NewRotatingByzantineLeaderDetector(
	self types.ProcessID,
	al p2p.Link,
	processes map[types.ProcessID]types.ProcessRank,
	faults int,
) *RotatingByzantineLeaderDetector {
	d := &RotatingByzantineLeaderDetector{
		self:      self,
		al:        al,
		processes: processes,
		faults:    faults,
		logger:    logger.NewNodeScopeLogger(self, logger.Scope{Key: "ld", Val: "rotating"}),
	}

	d.processesCount = len(processes)

	d.Deliverer = types.NewUnaryDeliverer(self)
	d.al.AddDeliverer(d)

	d.logger.Info().
		Int("processes", d.processesCount).
		Int("faults", faults).
		Msg("initialized rotating byzantine leader detector")

	return d
}

func (d *RotatingByzantineLeaderDetector) Init() {
	d.round = 1
	d.resetComplains()
	leader := d.currentLeader()
	d.logger.Info().
		Int("round", d.round).
		Str("leader", leader.String()).
		Msg("initialized, notifying initial leader")

	if d.receiver != nil {
		d.receiver.OnNewLeader(leader)
	}
}

func (d *RotatingByzantineLeaderDetector) Subscribe(r Receiver) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.receiver = r
}

func (d *RotatingByzantineLeaderDetector) OnComplain(pid types.ProcessID) {
	d.mu.Lock()
	currentLeader := d.currentLeader()
	if pid != currentLeader {
		d.logger.Warn().
			Str("complained_pid", pid.String()).
			Str("current_leader", currentLeader.String()).
			Int("round", d.round).
			Msg("ignoring complaint: target is not current leader")
		d.mu.Unlock()
		return
	}

	if d.complained {
		d.logger.Warn().
			Str("leader", pid.String()).
			Int("round", d.round).
			Msg("ignoring complaint: already complained this round")
		d.mu.Unlock()
		return
	}
	d.complained = true
	msg := d.makeComplainMsg()
	d.logger.Warn().
		Str("leader", pid.String()).
		Int("round", d.round).
		Msg("complaining about leader, broadcasting complaint")
	d.mu.Unlock()
	d.broadcast(msg)
}

func (d *RotatingByzantineLeaderDetector) Current() types.ProcessID {
	return d.currentLeader()
}

func (d *RotatingByzantineLeaderDetector) handleComplaint(from types.ProcessID, round int) {
	d.mu.Lock()
	if d.round != round {
		d.logger.Warn().
			Str("from", from.String()).
			Int("msg_round", round).
			Int("current_round", d.round).
			Msg("ignoring complaint: stale round")
		d.mu.Unlock()
		return
	}

	if _, existsComplain := d.complains[from]; existsComplain {
		d.logger.Warn().
			Str("from", from.String()).
			Int("msg_round", round).
			Int("current_round", d.round).
			Msg("ignoring complaint: duplicate")
		d.mu.Unlock()
		return
	}

	d.complains[from] = struct{}{}
	complainCount := len(d.complains)

	d.logger.Warn().
		Str("from", from.String()).
		Int("round", d.round).
		Int("complaints", complainCount).
		Int("faults", d.faults).
		Msg("recorded complaint")

	if complainCount > d.faults && !d.complained {
		d.complained = true
		msg := d.makeComplainMsg()
		d.logger.Info().
			Int("round", d.round).
			Int("complaints", complainCount).
			Int("threshold", d.faults).
			Msg("f+1 complaints received, joining complaint broadcast")
		d.mu.Unlock()
		d.broadcast(msg)
		return
	}

	if complainCount > 2*d.faults {
		prevRound := d.round
		d.round++
		d.resetComplains()
		leader := d.currentLeader()

		d.logger.Info().
			Int("prev_round", prevRound).
			Int("new_round", d.round).
			Str("new_leader", leader.String()).
			Int("complaints", complainCount).
			Int("threshold", 2*d.faults).
			Msg("2f+1 complaints reached, advancing round and electing new leader")

		d.mu.Unlock()
		if d.receiver != nil {
			d.receiver.OnNewLeader(leader)
		}
		return
	}
	d.mu.Unlock()
}

func (d *RotatingByzantineLeaderDetector) broadcast(msg types.Message) {
	d.logger.Info().
		Str("msg_type", msg.Name()).
		Int("targets", len(d.processes)).
		Msg("broadcasting message")

	for pid := range d.processes {
		d.al.Send(pid, msg)
	}
}

func (d *RotatingByzantineLeaderDetector) makeComplainMsg() types.Message {
	return ComplainMsg{
		BaseMsg: messages.NewBase(uuid.New(), d.self, ComplainMsgName),
		Round:   d.round,
	}
}

func (d *RotatingByzantineLeaderDetector) CalcLeader(ts int) types.ProcessID {
	return d.selectLeader(ts)
}

func (d *RotatingByzantineLeaderDetector) currentLeader() types.ProcessID {
	return d.selectLeader(d.round)
}

func (d *RotatingByzantineLeaderDetector) selectLeader(ts int) types.ProcessID {
	targetRank := (ts-1)%d.processesCount + 1
	var result types.ProcessID
	for pid, rank := range d.processes {
		if rank.Int() == targetRank {
			result = pid
		}
	}
	return result
}

func (d *RotatingByzantineLeaderDetector) resetComplains() {
	d.complains = make(map[types.ProcessID]struct{})
	d.complained = false
}

func (d *RotatingByzantineLeaderDetector) Deliver(message types.Message) {
	msg, ok := message.(ComplainMsg)
	if !ok {
		d.logger.Warn().Str("name", message.Name()).Msg("no complain msg")
		return
	}

	go d.handleComplaint(msg.From(), msg.Round)
}
