package failure

import (
	"context"
	"reliable/logger"
	"reliable/p2p"
	"reliable/types"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

type PerfectFailureDetector struct {
	types.Deliverer
	pl             p2p.Link
	ctx            context.Context
	correct        []types.ProcessID
	crashers       []types.Crasher
	inner          *perfectFailureDetector
	detectInterval time.Duration
	initOnce       sync.Once
	startOnce      sync.Once
}

func NewPerfectFailureDetector(
	ctx context.Context,
	id types.ProcessID,
	correct []types.ProcessID,
	pl p2p.Link,
	detectInterval time.Duration,
) *PerfectFailureDetector {
	pfd := &PerfectFailureDetector{
		Deliverer:      types.NewUnaryDeliverer(id),
		crashers:       make([]types.Crasher, 0),
		correct:        correct,
		ctx:            ctx,
		detectInterval: detectInterval,
	}

	pl.AddDeliverer(pfd)
	pfd.pl = pl
	return pfd
}

func (pfd *PerfectFailureDetector) Subscribe(crasher types.Crasher) {
	pfd.crashers = append(pfd.crashers, crasher)
}

func (pfd *PerfectFailureDetector) Init() {
	pfd.initOnce.Do(func() {
		pfd.reverseCrashers()
		pfd.inner = newPerfectFailureDetector(pfd.ctx, pfd.ProcessID(), pfd.correct, pfd, pfd.pl, pfd.detectInterval)
		pfd.inner.init()
	})
}

func (pfd *PerfectFailureDetector) Start() {
	pfd.startOnce.Do(func() {
		pfd.inner.start()
	})
}

func (pfd *PerfectFailureDetector) OnCrash(pid types.ProcessID) {
	for _, c := range pfd.crashers {
		c.OnCrash(pid)
	}
}

func (pfd *PerfectFailureDetector) Deliver(msg types.Message) {
	pfd.inner.Deliver(msg)
}

func (pfd *PerfectFailureDetector) reverseCrashers() {
	for i, j := 0, len(pfd.crashers)-1; i < j; i, j = i+1, j-1 {
		pfd.crashers[i], pfd.crashers[j] = pfd.crashers[j], pfd.crashers[i]
	}
}

func (pfd *PerfectFailureDetector) Stop() {
	pfd.inner.Crash()
}

type perfectFailureDetector struct {
	types.Crasher
	self                types.ProcessID
	ctx                 context.Context
	cancel              context.CancelFunc
	processes           map[types.ProcessID]struct{}
	alive               map[types.ProcessID]struct{}
	pl                  p2p.Link
	detected            map[types.ProcessID]struct{}
	healthCheckInterval time.Duration
	mu                  sync.RWMutex
	inner               types.Deliverer
	active              atomic.Bool
	logger              zerolog.Logger
}

func newPerfectFailureDetector(
	ctx context.Context,
	pid types.ProcessID,
	correct []types.ProcessID,
	crasher types.Crasher,
	pl p2p.Link,
	detectInterval time.Duration,
) *perfectFailureDetector {

	cctx, cancel := context.WithCancel(ctx)
	pfd := &perfectFailureDetector{
		self:                pid,
		ctx:                 cctx,
		cancel:              cancel,
		processes:           make(map[types.ProcessID]struct{}),
		alive:               map[types.ProcessID]struct{}{},
		Crasher:             crasher,
		healthCheckInterval: detectInterval,
		pl:                  pl,
		detected:            make(map[types.ProcessID]struct{}),
		logger:              logger.NewNodeScopeLogger(pid, logger.Scope{"failure", "pfd"}),
	}

	for _, p := range correct {
		pfd.alive[p] = struct{}{}
		pfd.processes[p] = struct{}{}
	}

	return pfd
}

func (pfd *perfectFailureDetector) init() {
	pfd.Recover()
}

func (pfd *perfectFailureDetector) start() {
	go pfd.background()
}

func (pfd *perfectFailureDetector) background() {
	timer := time.NewTimer(pfd.healthCheckInterval)
	defer timer.Stop()

	var iter int

	iter++
	pfd.detect(iter)
	for {
		select {
		case <-pfd.ctx.Done():
			return
		case <-timer.C:
			iter++
			pfd.detect(iter)
			timer.Reset(pfd.healthCheckInterval)
		}
	}
}

func (pfd *perfectFailureDetector) detect(iter int) {
	pfd.mu.Lock()

	oldAlive := pfd.alive
	pfd.alive = make(map[types.ProcessID]struct{})

	crashed := make([]types.ProcessID, 0)
	for pid := range pfd.processes {
		if pid == pfd.self {
			continue
		}
		if _, ok := oldAlive[pid]; !ok {
			if _, detected := pfd.detected[pid]; !detected {
				crashed = append(crashed, pid)
				pfd.detected[pid] = struct{}{}
			}
		}
	}

	pfd.mu.Unlock()

	for p := range pfd.processes {
		pfd.pl.Send(p, HeartbeatRequestMessage{
			id:   uuid.New(),
			from: pfd.Crasher.ProcessID(),
		})
	}

	for _, pid := range crashed {
		pfd.Crasher.OnCrash(pid)
	}

	//pfd.logger.Info().Int("iter", iter).Any("crashed", crashed).Any("alive", utils.KeysSlice(oldAlive)).Msg("finish")
}

func (pfd *perfectFailureDetector) Deliver(msg types.Message) {

	if !pfd.active.Load() {
		return
	}

	switch m := msg.(type) {
	case HeartbeatRequestMessage:
		pfd.pl.Send(msg.From(), HeartbeatResponseMessage{
			id:    uuid.New(),
			from:  pfd.Crasher.ProcessID(),
			reqId: m.ID(),
		})

	case HeartbeatResponseMessage:
		pfd.mu.Lock()
		pfd.alive[msg.From()] = struct{}{}
		//pfd.logger.Info().Str("pid", msg.From().String()).Msg("alive")
		pfd.mu.Unlock()
	}
}

func (pfd *perfectFailureDetector) Crash() {
	pfd.cancel()
	pfd.active.Store(false)
}

func (pfd *perfectFailureDetector) Recover() {
	pfd.active.Store(true)
}

func (pfd *perfectFailureDetector) AddDeliverer(d types.Deliverer) {}

func (pfd *perfectFailureDetector) Instance() string {
	return "pfd"
}
