package election

import "reliable/types"

type Election interface {
	types.Crasher
	types.Worker

	Subscribe(receiver Receiver)
}

type EventualElection interface {
	types.EventuallyCrasher
	types.Worker

	Subscribe(receiver Receiver)
}

type Receiver interface {
	OnNewLeader(pid types.ProcessID)
}
