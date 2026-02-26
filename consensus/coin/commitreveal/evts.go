package commitreveal

import (
	"reliable/types"
	"reliable/types/fsm"
)

type commitEvt struct {
	fsm.BaseEvent
}

type commitedEvt struct {
	fsm.BaseEvent
	commit []byte
	from   types.ProcessID
}

type revealEvt struct {
	fsm.BaseEvent
	from  types.ProcessID
	value types.Value
	salt  []byte
}
