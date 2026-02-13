package uniform

import "reliable/types"

type StartEpochEvt struct {
	types.NamedEvt
	Ts     int
	Leader types.ProcessID
}

type HandlerNewEpochEvt struct {
	types.NamedEvt
	Ts     int
	Leader types.ProcessID
}

type SendNAckEvt struct {
	types.NamedEvt
	to types.ProcessID
}
