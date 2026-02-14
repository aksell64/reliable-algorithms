package uniform

import (
	"reliable/types"
)

const (
	StateIdle     types.State = "idle"
	StatePropose  types.State = "propose"
	StateReading  types.State = "reading"
	StateWriting  types.State = "writing"
	StateDeciding types.State = "deciding"
	StateDecided  types.State = "decided"
	StateAborted  types.State = "aborted"
)

type initEvent struct {
	epochTs int
	leader  types.ProcessID
	current State
}

// Все входящие события — обёртки
type proposeEvent struct{ val types.Value }

type receivedReadEvent struct {
	msg StateMsg
}

type receivedAcceptEvent struct{}

type decideEvent struct{ val types.Value }

type decidedEvent struct{ val types.Value }
type abortEvent struct {
}

type writeEvent struct {
	state State
}

type handleReadEvent struct {
	from types.ProcessID
}

type handleWriteEvent struct {
	from types.ProcessID
	v    *types.Value
}

const (
	initEventName           = "init"
	proposeEventName        = "propose"
	receivedReadEventName   = "receivedReadEventName"
	receivedAcceptEventName = "receivedAcceptEventName"
	writeEventName          = "writeEventName"
	decideEventName         = "decideEventName"
	decidedEventName        = "decidedEventName"
	abortEventName          = "abort"
	handleReadEventName     = "handle_read"
	handleWriteEventName    = "handle_write"
)

func (initEvent) Tag() string           { return initEventName }
func (proposeEvent) Tag() string        { return proposeEventName }
func (receivedReadEvent) Tag() string   { return receivedReadEventName }
func (receivedAcceptEvent) Tag() string { return receivedAcceptEventName }
func (writeEvent) Tag() string          { return writeEventName }
func (decideEvent) Tag() string         { return decideEventName }
func (decidedEvent) Tag() string        { return decidedEventName }
func (abortEvent) Tag() string          { return abortEventName }
func (handleReadEvent) Tag() string     { return handleReadEventName }
func (handleWriteEvent) Tag() string    { return handleWriteEventName }

func initMachine(ec *epochConsensus, sm *types.StateMachine) {
	types.AddSmHandler(sm, StateIdle, initEventName, ec.onInit)
	types.AddSmHandler(sm, StatePropose, proposeEventName, ec.onPropose)
	types.AddSmHandler(sm, StateReading, receivedReadEventName, ec.onReceivedRead)
	types.AddSmHandler(sm, StateWriting, receivedAcceptEventName, ec.onReceivedAccept)
	types.AddSmHandler(sm, StateDeciding, decidedEventName, ec.onDecided)
	types.AddSmHandler(sm, StateIdle, decidedEventName, ec.onDecided)
	types.AddGlobalSmHandler(sm, handleReadEventName, ec.onHandleRead)
	types.AddGlobalSmHandler(sm, handleWriteEventName, ec.onHandleWrite)
}
