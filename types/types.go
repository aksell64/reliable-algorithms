package types

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	"github.com/google/uuid"
)

type ProcessID int

func (id ProcessID) String() string {
	return strconv.Itoa(int(id))
}

func (id ProcessID) Bytes() []byte {
	return []byte(id.String())
}

func IDFromBytes(byt []byte) (ProcessID, error) {
	pid, err := strconv.Atoi(string(byt))
	if err != nil {
		return ProcessID(0), err
	}
	return ProcessID(pid), nil
}

type ProcessRank int

func (r ProcessRank) String() string {
	return strconv.Itoa(int(r))
}

func (r ProcessRank) Int() int {
	return int(r)
}

type Value interface {
	fmt.Stringer
	Copy() Value

	Compare(other Value) bool
	Less(other Value) bool
}

func IntValue(i int) Value {
	return intValue(i)
}

type intValue int

func (v intValue) String() string {
	return strconv.Itoa(int(v))
}

func (v intValue) Copy() Value {
	return intValue(v)
}

func (v intValue) Compare(other Value) bool {
	ov, ok := other.(intValue)
	if !ok {
		return false
	}
	return v == ov
}

func (v intValue) Less(other Value) bool {
	ov, ok := other.(intValue)
	if !ok {
		return false
	}

	return v < ov
}

type Message interface {
	ID() uuid.UUID
	Name() string
	From() ProcessID
}

type Crasher interface {
	OnCrash(id ProcessID)
	ProcessID() ProcessID
}

type EventuallyCrasher interface {
	OnSuspectCrashed(id ProcessID)
	OnRestoreCrashed(id ProcessID)
	ID() ProcessID
}

type Layer interface {
	Deliverer
	Worker
}

type Worker interface {
	Init()
	Start()
	Stop()
}

func InitWorkers(workers ...Worker) {
	for _, w := range workers {
		w.Init()
	}
}

func StartWorkers(workers ...Worker) {
	for _, w := range workers {
		w.Start()
	}
}

func StopWorkers(workers ...Worker) {
	for _, w := range workers {
		w.Stop()
	}
}

type WorkerOnce struct {
	initOnce  sync.Once
	startOnce sync.Once
	stopOnce  sync.Once
}

func NewWorkerOnce() *WorkerOnce {
	return &WorkerOnce{}
}

func (w *WorkerOnce) Init(f func()) {
	w.initOnce.Do(f)
}

func (w *WorkerOnce) Start(f func()) {
	w.startOnce.Do(f)
}

func (w *WorkerOnce) Stop(f func()) {
	w.stopOnce.Do(f)
}

type RuntimeEvent interface {
	Name() string
}

type NamedEvt struct {
	name string
}

func NamedWithScopes(name string, scopes ...string) NamedEvt {
	n := name
	for _, scope := range scopes {
		n += "_" + scope
	}
	return NamedEvt{n}
}

func (evt NamedEvt) Name() string {
	return evt.name
}

type RuntimeHandleResult struct {
	ShouldStop bool
}

type RuntimeEventHandler func(evt RuntimeEvent) RuntimeHandleResult

type Runtime struct {
	log      *appendOnlyRuntimeLog
	readIdx  int
	handlers map[string]RuntimeEventHandler
	mu       sync.RWMutex
}

func NewRuntime() *Runtime {
	return &Runtime{
		log:      newAppendOnlyRuntimeLog(),
		handlers: make(map[string]RuntimeEventHandler),
	}
}

func AddRuntimeHandler[Event RuntimeEvent](r *Runtime, h func(evt Event) RuntimeHandleResult) {
	hh := func(evt RuntimeEvent) RuntimeHandleResult {
		return h(evt.(Event))
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	var evt Event
	r.handlers[evt.Name()] = hh
}

func (r *Runtime) Push(evt RuntimeEvent) {
	r.mu.Lock()
	r.log.append(evt)
	r.mu.Unlock()
}

func (r *Runtime) Handle(evt RuntimeEvent) RuntimeHandleResult {
	r.mu.RLock()
	h, ok := r.handlers[evt.Name()]
	if !ok {
		r.mu.RUnlock()
		return RuntimeHandleResult{false}
	}
	r.mu.RUnlock()
	return h(evt)
}

func (r *Runtime) ReadUncommited() []RuntimeEvent {
	r.mu.Lock()
	defer r.mu.Unlock()

	evts := r.log.read(r.readIdx, len(*r.log))
	r.readIdx = len(*r.log)
	return evts
}

type appendOnlyRuntimeLog []RuntimeEvent

func newAppendOnlyRuntimeLog() *appendOnlyRuntimeLog {
	l := make(appendOnlyRuntimeLog, 0)
	return &l
}

func (l *appendOnlyRuntimeLog) append(evt RuntimeEvent) {
	*l = append(*l, evt)
}

func (l *appendOnlyRuntimeLog) read(from, to int) []RuntimeEvent {
	evts := (*l)[from:to]
	return evts
}

type RuntimeProcessor struct {
	ctx     context.Context
	workers []Worker
	r       *Runtime
}

func NewRuntimeProcessor(ctx context.Context, r *Runtime, w ...Worker) *RuntimeProcessor {
	if r == nil {
		r = NewRuntime()
	}
	p := &RuntimeProcessor{
		ctx:     ctx,
		workers: w,
		r:       r,
	}
	go p.background()
	return p
}

func (proc *RuntimeProcessor) AddWorker(w Worker) {
	proc.workers = append(proc.workers, w)
}

func (proc *RuntimeProcessor) ProcessEvent(evt RuntimeEvent) {
	proc.r.Push(evt)
}

func (proc *RuntimeProcessor) background() {
	for proc.ctx.Err() == nil {
		evts := proc.r.ReadUncommited()
		for _, evt := range evts {
			proc.processEvent(evt)
		}
	}
}

func (proc *RuntimeProcessor) processEvent(evt RuntimeEvent) {
	res := proc.r.Handle(evt)

	if res.ShouldStop {
		for _, w := range proc.workers {
			go w.Stop()
		}
	}
}
