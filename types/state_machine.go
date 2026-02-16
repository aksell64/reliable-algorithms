package types

type State string

func (s State) String() string {
	return string(s)
}

const SameState = State("same")

type Handler func(ev Event) State

type Event interface {
	Tag() string
}

type StateMachine struct {
	state          State
	handlers       map[State]map[string]Handler
	globalHandlers map[string]Handler
}

func NewStateMachine() *StateMachine {
	return &StateMachine{
		handlers:       map[State]map[string]Handler{},
		globalHandlers: map[string]Handler{},
	}
}

func (sm *StateMachine) SetState(state State) {
	sm.state = state
}

func (sm *StateMachine) addHandler(state State, evtTag string, h Handler) {
	if _, ok := sm.handlers[state]; !ok {
		sm.handlers[state] = make(map[string]Handler)
	}
	sm.handlers[state][evtTag] = h
}

func (sm *StateMachine) addGlobalHandler(evtTag string, h Handler) {
	sm.globalHandlers[evtTag] = h
}

func (sm *StateMachine) Apply(evt Event) {
	global, ok := sm.globalHandlers[evt.Tag()]
	if ok {
		global(evt)
	}

	transitions, ok := sm.handlers[sm.state]
	if !ok {
		return
	}
	handler, ok := transitions[evt.Tag()]
	if !ok {
		return
	}
	state := handler(evt)
	if state != SameState {
		sm.state = state
	}
}

func (sm *StateMachine) Current() State {
	return sm.state
}

func AddSmHandler[T Event](sm *StateMachine, state State, evtTag string, h func(evt T) State) {
	hh := func(evt Event) State { return h(evt.(T)) }
	sm.addHandler(state, evtTag, hh)
}

func AddGlobalSmHandler[T Event](sm *StateMachine, evtTag string, h func(evt T) State) {
	hh := func(evt Event) State { return h(evt.(T)) }
	sm.addGlobalHandler(evtTag, hh)
}
