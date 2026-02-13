package uniform

import (
	"reliable/types"
)

const (
	NewEpochMsgName = "new_epoch"
	NAckMsgName     = "nack"
)

type NewEpochMsg struct {
	types.Message
	Ts int
}

type NAck struct {
	types.Message
}
