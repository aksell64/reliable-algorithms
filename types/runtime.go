package types

import (
	"context"
	"sync"
)

type RuntimeEvent interface {
	Name() string
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
			res := proc.r.Handle(evt)
			proc.handleResult(res)
		}
	}
}

func (proc *RuntimeProcessor) handleResult(res RuntimeHandleResult) {
	if res.ShouldStop {
		for _, w := range proc.workers {
			go w.Stop()
		}
	}
}
