package election

import (
	"context"
	"reliable/database"
	"reliable/logger"
	"reliable/messages"
	"reliable/p2p"
	"reliable/types"
	"reliable/utils"
	"reliable/utils/codec"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

const (
	leeEpochStorageKey = "lee_epoch"
)

type EpochCandidate struct {
	Epoch   int
	Process types.ProcessID
}

type LowerEpochElection struct {
	types.Deliverer
	ctx            context.Context
	cancel         context.CancelFunc
	self           types.ProcessID
	processes      map[types.ProcessID]types.ProcessRank
	minRankPID     types.ProcessID
	minRank        types.ProcessRank
	epoch          int
	leader         types.ProcessID
	candidates     map[types.ProcessID]int
	candidatesLock sync.RWMutex
	storage        database.KVStore
	fl             p2p.Link
	receivers      []Receiver
	delayDelta     time.Duration
	delay          time.Duration
	logger         zerolog.Logger
	runtime        *types.RuntimeProcessor

	once   *types.WorkerOnce
	stopCh chan struct{}
	mu     sync.Mutex
}

func NewLowerEpochElection(
	ctx context.Context,
	self types.ProcessID,
	processes map[types.ProcessID]types.ProcessRank,
	storage database.KVStore,
	fl p2p.Link,
	delayDelta time.Duration,
	registry *codec.Registry,
	runtime *types.RuntimeProcessor,
) *LowerEpochElection {
	e := new(LowerEpochElection)
	e.ctx, e.cancel = context.WithCancel(ctx)
	e.Deliverer = types.NewUnaryDeliverer(self)
	e.self = self
	e.processes = make(map[types.ProcessID]types.ProcessRank)
	e.candidates = make(map[types.ProcessID]int)
	e.storage = storage
	e.receivers = make([]Receiver, 0)
	e.delayDelta = delayDelta
	fl.AddDeliverer(e)
	e.fl = fl
	e.stopCh = make(chan struct{})
	e.logger = logger.NewNodeScopeLogger(self, logger.Scope{"election", "lee"})
	//e.logger = zerolog.Nop()
	e.once = types.NewWorkerOnce()

	codec.RegisterTyped[HeartbeatMessage](registry)
	if runtime == nil {
		runtime = types.NewRuntimeProcessor(ctx, types.NewRuntime())
	}
	e.runtime = runtime
	e.runtime.AddWorker(e)
	for pid, rank := range processes {
		e.processes[pid] = rank
	}
	return e
}

func (e *LowerEpochElection) Init() {
	e.once.Init(func() {
		e.fl.Init()
	})
}

func (e *LowerEpochElection) Start() {
	e.once.Start(func() {
		e.epoch = 0
		e.storeEpoch()
		e.candidates = make(map[types.ProcessID]int)
		e.fl.Start()
		e.Recovery()
	})
}

func (e *LowerEpochElection) Stop() {
	e.once.Stop(func() {
		e.fl.Stop()
		e.cancel()
		<-e.stopCh
		e.logger.Warn().Msg("stopped")
	})
}

func (e *LowerEpochElection) Subscribe(receiver Receiver) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.receivers = append(e.receivers, receiver)
}

func (e *LowerEpochElection) Recovery() {
	e.mu.Lock()
	minPID, _ := e.minProcessesRank()
	e.leader = minPID
	e.sendLeader()
	e.delay = e.delayDelta
	e.retrieveEpoch()
	e.epoch++
	e.storeEpoch()
	e.candidates = make(map[types.ProcessID]int)

	e.stopCh = make(chan struct{})
	cctx, cancel := context.WithCancel(e.ctx)
	e.cancel = cancel
	e.mu.Unlock()

	go e.background(cctx)
}

func (e *LowerEpochElection) background(ctx context.Context) {
	msg := HeartbeatMessage{
		BaseMsg: messages.NewBase(uuid.New(), e.self, "hb"),
		Epoch:   e.epoch,
		SentAt:  time.Now(),
	}
	e.sendEpochHeartbeat(msg)

	timer := time.NewTimer(e.delay)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			close(e.stopCh)
			return
		case <-timer.C:
			e.tryElection()
			timer.Reset(e.delay)
		}
	}
}

func (e *LowerEpochElection) tryElection() {
	e.mu.Lock()

	e.candidatesLock.Lock()
	candidates := make(map[types.ProcessID]int)
	for pid, epoche := range e.candidates {
		candidates[pid] = epoche
	}
	e.candidates = make(map[types.ProcessID]int)
	e.candidatesLock.Unlock()

	newLeader, emptyCandidates := e.selectMinEpochLeader(candidates)

	e.runtime.ProcessEvent(ElectionEvt{
		CurrentLeader: e.leader,
		CurrentDelay:  e.delay,
		NewLeader:     newLeader,
		Self:          e.self,
	})

	delayChanged := false
	if newLeader != e.leader {
		e.delay += e.delayDelta
		delayChanged = true
		oldLeader := e.leader
		e.leader = newLeader

		e.logger.Info().
			Int("epoch", e.epoch).
			Int("newLeader", int(e.leader)).
			Int("oldLeader", int(oldLeader)).
			Int64("curDelay", e.delay.Milliseconds()).
			Int("count candidates", len(candidates)).
			Msg("leader elected")

		e.sendLeader()
	}

	if emptyCandidates && !delayChanged {
		e.delay += e.delayDelta
		delayChanged = true
	}

	msg := HeartbeatMessage{
		BaseMsg: messages.NewBase(uuid.New(), e.self, "hb"),
		Epoch:   e.epoch,
		SentAt:  time.Now(),
	}

	e.mu.Unlock()

	e.sendEpochHeartbeat(msg)
}

func (e *LowerEpochElection) selectMinEpochLeader(candidates map[types.ProcessID]int) (types.ProcessID, bool) {
	if len(candidates) == 0 {
		return e.self, true
	}

	minEpoch := slices.Min(utils.ValuesSlice(candidates))
	minEpochPIDs := make([]types.ProcessID, 0, len(candidates))
	for pid, epoch := range candidates {
		if epoch == minEpoch {
			minEpochPIDs = append(minEpochPIDs, pid)
		}
	}

	if len(minEpochPIDs) == 1 {
		return minEpochPIDs[0], false
	}

	slices.Sort(minEpochPIDs)

	first := minEpochPIDs[0]
	minRankPID, minRank := first, e.processes[first]
	for _, pid := range minEpochPIDs[1:] {
		rank := e.processes[pid]
		if rank <= minRank {
			minRank = rank
			minRankPID = pid
		}
	}

	return minRankPID, false
}

func (e *LowerEpochElection) storeEpoch() {
	e.storage.Set(leeEpochStorageKey, []byte(strconv.Itoa(e.epoch)))
}

func (e *LowerEpochElection) retrieveEpoch() {
	epochBytes, exists := e.storage.Get(leeEpochStorageKey)
	if !exists {
		e.epoch = 0
		return
	}

	epoch, err := strconv.ParseInt(string(epochBytes), 10, 64)
	if err != nil {
		return
	}

	e.epoch = int(epoch)
}

func (e *LowerEpochElection) sendLeader() {
	leader := e.leader
	for _, receiver := range e.receivers {
		receiver.OnNewLeader(leader)
	}
}

func (e *LowerEpochElection) sendEpochHeartbeat(msg HeartbeatMessage) {
	for pid := range e.processes {
		e.fl.Send(pid, msg)
	}
}

func (e *LowerEpochElection) Deliver(msg types.Message) {
	hmsg, ok := msg.(HeartbeatMessage)
	if !ok {
		return
	}

	e.handleHeartbeat(hmsg)
}

func (e *LowerEpochElection) handleHeartbeat(msg HeartbeatMessage) {
	e.candidatesLock.Lock()
	defer e.candidatesLock.Unlock()

	curEpoch, ok := e.candidates[msg.From()]
	if ok && curEpoch >= msg.Epoch {
		return
	}

	e.candidates[msg.From()] = msg.Epoch
}

func (e *LowerEpochElection) minProcessesRank() (types.ProcessID, types.ProcessRank) {
	curRank := e.processes[e.self]
	curPID := e.self
	for pid, rank := range e.processes {
		if rank <= curRank {
			curPID = pid
			curRank = rank
		}
	}
	return curPID, curRank
}
