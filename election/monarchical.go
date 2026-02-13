package election

import (
	"context"
	"reliable/failure"
	"reliable/types"
	"reliable/utils"
	"sync"
	"time"
)

type monarchicalElection struct {
	ctx            context.Context
	cancel         context.CancelFunc
	self           types.ProcessID
	pfd            *failure.PerfectFailureDetector
	processesRanks map[types.ProcessID]types.ProcessRank
	suspected      map[types.ProcessID]struct{}
	processes      map[types.ProcessID]struct{}
	leader         *types.ProcessID
	receivers      []Receiver
	once           *types.WorkerOnce
	mu             sync.RWMutex
}

func NewMonarchicalElection(
	ctx context.Context,
	self types.ProcessID,
	pfd *failure.PerfectFailureDetector,
	processes map[types.ProcessID]types.ProcessRank,
) Election {
	e := new(monarchicalElection)
	e.ctx, e.cancel = context.WithCancel(ctx)
	e.self = self
	e.processesRanks = make(map[types.ProcessID]types.ProcessRank)
	e.processes = make(map[types.ProcessID]struct{})
	e.suspected = make(map[types.ProcessID]struct{})
	e.receivers = make([]Receiver, 0)
	e.once = types.NewWorkerOnce()

	for pid, rank := range processes {
		e.processesRanks[pid] = rank
		e.processes[pid] = struct{}{}
	}

	e.pfd = pfd

	return e
}

func (e *monarchicalElection) ProcessID() types.ProcessID {
	return e.self
}

func (e *monarchicalElection) Subscribe(receiver Receiver) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.receivers = append(e.receivers, receiver)
}

func (e *monarchicalElection) OnCrash(pid types.ProcessID) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.suspected[pid] = struct{}{}
}

func (e *monarchicalElection) Init() {
	e.once.Init(func() {
		e.pfd.Subscribe(e)
		e.pfd.Init()
	})
}

func (e *monarchicalElection) Start() {
	e.once.Start(func() {
		e.pfd.Start()
		e.tryElection()
		go e.background()
	})
}

func (e *monarchicalElection) Stop() {
	e.once.Stop(func() {
		e.pfd.Stop()
		e.cancel()
	})
}

func (e *monarchicalElection) background() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			return

		case <-ticker.C:
			e.tryElection()
		}
	}
}

func (e *monarchicalElection) tryElection() {
	e.mu.Lock()

	var newLeader *types.ProcessID
	maxPID := e.maxAliveProcessRank()

	if e.leader == nil {
		newLeader = &maxPID
	} else if maxPID != *e.leader {
		newLeader = &maxPID
	}

	if newLeader == nil {
		e.mu.Unlock()
		return
	}

	e.leader = newLeader
	e.mu.Unlock()

	for _, receiver := range e.receivers {
		go receiver.OnNewLeader(*e.leader)
	}
}

func (e *monarchicalElection) maxAliveProcessRank() types.ProcessID {
	diff := utils.Difference(e.processes, e.suspected)
	_, pid := e.maxRank(diff)
	return pid
}

func (e *monarchicalElection) maxRank(procs map[types.ProcessID]struct{}) (types.ProcessRank, types.ProcessID) {
	var (
		maxPID  types.ProcessID
		maxRank types.ProcessRank = -1
	)

	for pid, _ := range procs {
		rank, ok := e.processesRanks[pid]
		if !ok {
			continue
		}

		if maxRank < rank {
			maxRank = rank
			maxPID = pid
		}
	}

	return maxRank, maxPID
}
