package uniformhierarchical

import (
	"reliable/types"
	"sync"
)

type Runtime struct {
	log      []types.RuntimeEvent
	handlers map[string]EvtHandler
	mu       sync.Mutex
}

func NewRuntime() *Runtime {
	return &Runtime{
		log:      make([]types.RuntimeEvent, 0),
		handlers: make(map[string]EvtHandler),
	}
}

func (r *Runtime) CheckStop(evt types.RuntimeEvent) bool {
	result := r.Upon(evt)
	return result.ShouldStop
}

func (r *Runtime) Upon(evt types.RuntimeEvent) HandleResult {
	r.mu.Lock()
	r.log = append(r.log, evt)
	handler, ok := r.handlers[evt.Name()]
	if !ok {
		r.mu.Unlock()
		return HandleResult{}
	}
	r.mu.Unlock()
	res := handler(evt)
	return res
}

type HandleResult struct {
	ShouldStop bool
}

type EvtHandler func(evt types.RuntimeEvent) HandleResult

func AddHandler[T types.RuntimeEvent](run *Runtime, f func(evt T) HandleResult) {
	evtHandler := func(evt types.RuntimeEvent) HandleResult {
		return f(evt.(T))
	}
	var evt T
	run.handlers[evt.Name()] = evtHandler
}

type HandleAckEvt struct {
	PID         types.ProcessID
	From        types.ProcessID
	Round       int
	ReceivedAck int
}

func (e HandleAckEvt) Name() string {
	return "HandleAckEvt"
}

type ReadyDecideEvt struct {
	PID     types.ProcessID
	Decided types.Value
}

func (e ReadyDecideEvt) Name() string {
	return "ReadyDecideEvt"
}
