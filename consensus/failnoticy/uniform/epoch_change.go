package uniform

import (
	"context"
	"reliable/broadcaster"
	"reliable/election"
	"reliable/logger"
	"reliable/messages"
	"reliable/p2p"
	"reliable/types"
	"reliable/utils"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

const (
	NewEpochMsgName = "new_epoch"
	NAckMsgName     = "nack"

	bcastNewEpochDebounceInterval    = 300 * time.Millisecond
	bcastNewEpochMaxDebounceInterval = bcastNewEpochDebounceInterval * 4
)

type NewEpochMsg struct {
	types.Message
	Ts int
}

type NAckMsg struct {
	types.Message
}

type EpochStarter interface {
	StartEpoch(ts int, leader types.ProcessID)
}

type LeaderBasedEpochChanger struct {
	types.Deliverer
	ctx             context.Context
	starter         EpochStarter
	trusted         types.ProcessID
	lastTs          int
	self            types.ProcessID
	processes       map[types.ProcessID]types.ProcessRank
	processesCount  int
	ts              int
	beb             broadcaster.Broadcaster
	election        *election.LowerEpochElection
	pl              p2p.Link
	once            *types.WorkerOnce
	runtime         *types.RuntimeProcessor
	mu              sync.RWMutex
	bcastNewEpochCh chan NewEpochMsg
	logger          zerolog.Logger
}

func NewLeaderBasedEpochChanger(
	ctx context.Context,
	self types.ProcessID,
	processes map[types.ProcessID]types.ProcessRank,
	beb broadcaster.Broadcaster,
	election *election.LowerEpochElection,
	pl p2p.Link,
	runtime *types.Runtime,
) *LeaderBasedEpochChanger {
	ec := new(LeaderBasedEpochChanger)
	ec.processes = processes
	ec.beb = beb
	ec.pl = pl
	ec.election = election
	ec.self = self
	ec.ctx = ctx
	ec.Deliverer = types.NewUnaryDeliverer(self)
	ec.processesCount = len(processes)
	ec.bcastNewEpochCh = make(chan NewEpochMsg, ec.processesCount)

	ec.beb.AddDeliverer(ec, types.DelivererWithMsgNames(NewEpochMsgName))
	ec.pl.AddDeliverer(ec, types.DelivererWithMsgNames(NAckMsgName))

	ec.runtime = types.NewRuntimeProcessor(ctx, runtime, ec)
	ec.once = types.NewWorkerOnce()
	ec.logger = logger.NewNodeScopeLogger(self, logger.Scope{"epoch_changer", "leader_based"})

	return ec
}

func (ec *LeaderBasedEpochChanger) Init() {
	ec.once.Init(func() {
		ec.beb.Init()
		ec.pl.Init()
		ec.election.Init()
		maxPID := slices.Max(utils.KeysSlice(ec.processes))
		ec.init(maxPID)
	})
}

func (ec *LeaderBasedEpochChanger) Start() {
	ec.once.Start(func() {
		ec.beb.Start()
		ec.pl.Start()
		ec.election.Subscribe(ec)
		ec.election.Start()
		go ec.background()
	})
}

func (ec *LeaderBasedEpochChanger) Stop() {
	ec.once.Stop(func() {
		ec.beb.Stop()
		ec.pl.Stop()
		ec.election.Stop()
	})
}

func (ec *LeaderBasedEpochChanger) SetEpochStarter(starter EpochStarter) {
	ec.mu.Lock()
	defer ec.mu.Unlock()
	ec.starter = starter
}

func (ec *LeaderBasedEpochChanger) init(leader types.ProcessID) {
	ec.trusted = leader
	ec.lastTs = 0
	rank := ec.processes[ec.self]
	ec.ts = rank.Int()
	ec.logger.Info().Int("ts", ec.ts).Msg("init")
}

func (ec *LeaderBasedEpochChanger) background() {
	debounceTimer := time.NewTimer(0)
	debounceTimer.Stop()

	maxWaitTimer := time.NewTimer(0)
	maxWaitTimer.Stop()

	var pending *NewEpochMsg

	// tryBroadcast flushes the pending message (if any) and resets both timers.
	// Only the latest message is broadcast since newer epochs supersede older ones.
	tryBroadcast := func() {
		debounceTimer.Stop()
		maxWaitTimer.Stop()
		if pending != nil {
			msg := *pending
			pending = nil
			go ec.beb.Broadcast(ec.ctx, msg)
		}
	}

	for {
		select {
		case <-ec.ctx.Done():
			debounceTimer.Stop()
			maxWaitTimer.Stop()
			return

		case msg := <-ec.bcastNewEpochCh:
			wasPending := pending != nil
			pending = &msg

			// Debounce: reset the short timer on every incoming message
			// so that a rapid burst collapses into a single broadcast.
			debounceTimer.Stop()
			debounceTimer.Reset(bcastNewEpochDebounceInterval)

			// Max wait: start the upper-bound timer only on the first message
			// in a series to prevent starvation under continuous NAck flow.
			if !wasPending {
				maxWaitTimer.Reset(bcastNewEpochMaxDebounceInterval)
			}

		// NAck storm settled — broadcast the latest epoch.
		case <-debounceTimer.C:
			tryBroadcast()

		// NAcks keep coming non-stop — force broadcast to avoid starvation.
		case <-maxWaitTimer.C:
			tryBroadcast()
		}
	}
}

func (ec *LeaderBasedEpochChanger) triggerBroadcastNewEpoch(msg NewEpochMsg) {
	select {
	case ec.bcastNewEpochCh <- msg:
	case <-ec.ctx.Done():
	}
}

func (ec *LeaderBasedEpochChanger) OnNewLeader(leader types.ProcessID) {
	ec.mu.Lock()
	//ec.logger.Info().Str("leader", leader.String()).Msg("new leader")
	ec.trusted = leader
	if ec.trusted != ec.self {
		ec.mu.Unlock()
		return
	}

	ec.ts += ec.processesCount
	ec.logger.Info().Int("ts", ec.ts).Msg("make epoch")

	msg := NewEpochMsg{
		Ts:      ec.ts,
		Message: messages.NewBase(uuid.New(), ec.self, "new_epoch"),
	}
	ec.mu.Unlock()

	ec.triggerBroadcastNewEpoch(msg)
}

func (ec *LeaderBasedEpochChanger) handleNewEpoch(leader types.ProcessID, ts int) {
	ec.mu.Lock()

	if leader == ec.trusted && ts > ec.lastTs {
		oldLastTs := ec.lastTs
		ec.lastTs = ts
		starter := ec.starter
		ec.mu.Unlock()

		if starter != nil {
			starter.StartEpoch(ts, leader)
		}

		ec.logger.Info().
			Int("ts", ts).
			Int("oldLastTs", oldLastTs).
			Int("lastTs", ec.lastTs).
			Str("leader", leader.String()).
			Msg("new leader epoch")
		return
	}

	//ec.logger.Info().
	//	Int("ts", ts).
	//	Int("curLastTs", ec.lastTs).
	//	Str("leader", leader.String()).
	//	Str("curLeader", ec.trusted.String()).
	//	Msg("incorrect leader epoch")

	ec.mu.Unlock()

	msg := NAckMsg{
		Message: messages.NewBase(uuid.New(), ec.self, NAckMsgName),
	}
	ec.pl.Send(leader, msg)
}

func (ec *LeaderBasedEpochChanger) handleNAck() {
	ec.mu.Lock()
	if ec.trusted != ec.self {
		ec.mu.Unlock()
		return
	}

	ec.ts += ec.processesCount
	//ec.logger.Info().Int("ts", ec.ts).Msg("make nack epoch")

	msg := NewEpochMsg{
		Ts:      ec.ts,
		Message: messages.NewBase(uuid.New(), ec.self, NewEpochMsgName),
	}
	ec.mu.Unlock()

	ec.triggerBroadcastNewEpoch(msg)
}

func (ec *LeaderBasedEpochChanger) Deliver(msg types.Message) {
	switch m := msg.(type) {
	case NewEpochMsg:
		ec.handleNewEpoch(m.From(), m.Ts)
	case NAckMsg:
		ec.handleNAck()
	}
}
