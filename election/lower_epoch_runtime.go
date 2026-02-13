package election

import (
	"reliable/types"
	"time"
)

type ElectionEvt struct {
	CurrentLeader types.ProcessID
	CurrentDelay  time.Duration
	NewLeader     types.ProcessID
	Self          types.ProcessID
}

func (evt ElectionEvt) Name() string {
	return "election_evt"
}
