package failure

import (
	"context"
	"reliable/p2p"
	"reliable/types"
	"reliable/utils"
	"sync"
	"time"

	"github.com/google/uuid"
)

type EventuallyPerfectFailureDetector struct {
	types.Deliverer
	ctx      context.Context
	cancel   context.CancelFunc
	self     types.ProcessID
	crashers []types.EventuallyCrasher
	pl       p2p.Link

	processes      map[types.ProcessID]struct{}
	alive          map[types.ProcessID]struct{}
	suspected      map[types.ProcessID]struct{}
	delay          time.Duration
	delayDelta     time.Duration
	heartbeatDelay *time.Duration
	mu             sync.RWMutex
	once           *types.WorkerOnce
}

func NewEventuallyPerfectFailureDetector(
	ctx context.Context,
	self types.ProcessID,
	pl p2p.Link,
	delayDelta time.Duration,
) *EventuallyPerfectFailureDetector {
	pfd := new(EventuallyPerfectFailureDetector)
	pfd.ctx, pfd.cancel = context.WithCancel(ctx)
	pfd.self = self
	pfd.delayDelta = delayDelta
	pfd.processes = make(map[types.ProcessID]struct{})
	pfd.alive = make(map[types.ProcessID]struct{})
	pfd.suspected = make(map[types.ProcessID]struct{})
	pfd.crashers = make([]types.EventuallyCrasher, 0)
	pfd.pl = pl
	pfd.pl.AddDeliverer(pfd)
	pfd.Deliverer = types.NewUnaryDeliverer(self)
	pfd.once = types.NewWorkerOnce()
	return pfd
}

func (pfd *EventuallyPerfectFailureDetector) Init() {
}

func (pfd *EventuallyPerfectFailureDetector) Start() {
	pfd.once.Start(func() {
		pfd.incDelay()
		go pfd.background()
	})
}

func (pfd *EventuallyPerfectFailureDetector) Stop() {
	pfd.once.Stop(func() {
		pfd.cancel()
	})
}

func (pfd *EventuallyPerfectFailureDetector) Subscribe(crasher types.EventuallyCrasher) {
	pfd.mu.Lock()
	defer pfd.mu.Unlock()
	pfd.crashers = append(pfd.crashers, crasher)
}

func (pfd *EventuallyPerfectFailureDetector) background() {
	timer := time.NewTimer(pfd.delay)
	defer timer.Stop()

	for {
		select {
		case <-pfd.ctx.Done():
			return
		case <-timer.C:
			pfd.detect()
			timer.Reset(pfd.delay)
		}
	}
}

func (pfd *EventuallyPerfectFailureDetector) detect() {
	procs := utils.KeysSlice(pfd.processes)
	oldAlive := pfd.alive
	toSuspect := make([]types.ProcessID, 0)
	toRestore := make([]types.ProcessID, 0)

	pfd.mu.Lock()
	if len(utils.Intersection(oldAlive, pfd.suspected)) != 0 {
		pfd.incDelay()
	}

	for _, pid := range procs {
		_, isAlive := oldAlive[pid]
		_, isSuspected := pfd.suspected[pid]

		if !isAlive && !isSuspected {
			pfd.suspected[pid] = struct{}{}
			toSuspect = append(toSuspect, pid)
			continue
		}

		if isAlive && isSuspected {
			delete(pfd.suspected, pid)
			toRestore = append(toRestore, pid)
			continue
		}
	}
	pfd.alive = make(map[types.ProcessID]struct{})
	pfd.mu.Unlock()

	for _, p := range toSuspect {
		for _, crasher := range pfd.crashers {
			crasher.OnSuspectCrashed(p)
		}
	}

	for _, p := range toRestore {
		for _, crasher := range pfd.crashers {
			crasher.OnRestoreCrashed(p)
		}
	}

	for _, p := range procs {
		msg := HeartbeatRequestMessage{
			id:   uuid.New(),
			from: pfd.self,
		}
		go pfd.pl.Send(p, msg)
	}
}

func (pfd *EventuallyPerfectFailureDetector) Deliver(msg types.Message) {
	switch msg.(type) {
	case HeartbeatRequestMessage:

		pfd.pl.Send(msg.From(), HeartbeatResponseMessage{
			id:    uuid.New(),
			from:  pfd.self,
			reqId: msg.ID(),
		})

	case HeartbeatResponseMessage:
		pfd.alive[msg.From()] = struct{}{}
	}
}

func (pfd *EventuallyPerfectFailureDetector) incDelay() {
	pfd.delay += pfd.delayDelta
}
