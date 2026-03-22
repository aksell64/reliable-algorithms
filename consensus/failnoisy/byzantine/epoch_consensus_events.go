package byzantine

import (
	"reliable/types"
	"reliable/types/fsm"
)

const (
	proposeEvtTag = "propose"
	readEvtTag    = "read"
	collectEvtTag = "collect"
	writeEvtTag   = "write"
	acceptEvtTag  = "accept"
)

type proposeEvt struct {
	fsm.BaseEvent
	val types.Value
}

func newProposeEvt(val types.Value) fsm.Event {
	return proposeEvt{
		BaseEvent: fsm.NewBaseEvent(proposeEvtTag),
		val:       val,
	}
}

type readEvt struct {
	fsm.BaseEvent
	from types.ProcessID
}

func newReadEvt(from types.ProcessID) fsm.Event {
	return readEvt{
		BaseEvent: fsm.NewBaseEvent(readEvtTag),
		from:      from,
	}
}

type collectedEvt struct {
	fsm.BaseEvent
	states []State
}

func newCollectedEvt(states []State) fsm.Event {
	return collectedEvt{
		BaseEvent: fsm.NewBaseEvent(collectEvtTag),
		states:    states,
	}
}

type writeEvt struct {
	fsm.BaseEvent
	val  types.Value
	from types.ProcessID
}

func newWriteEvt(from types.ProcessID, val types.Value) fsm.Event {
	return writeEvt{
		BaseEvent: fsm.NewBaseEvent(writeEvtTag),
		val:       val,
		from:      from,
	}
}

type acceptEvt struct {
	fsm.BaseEvent
	from types.ProcessID
	val  types.Value
}

func newAcceptEvt(from types.ProcessID, val types.Value) fsm.Event {
	return acceptEvt{
		BaseEvent: fsm.NewBaseEvent(acceptEvtTag),
		from:      from,
		val:       val,
	}
}
