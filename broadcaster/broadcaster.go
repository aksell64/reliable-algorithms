package broadcaster

import (
	"context"
	"reliable/types"
)

type Broadcaster interface {
	types.Layer
	Broadcast(ctx context.Context, msg types.Message)
	AddCorrect(pid types.ProcessID)
	RemoveCorrect(id types.ProcessID)
}

type BroadcastContext struct {
	AlreadySent []types.ProcessID
	CurrentSend types.ProcessID
	NodesCount  int
	SelfID      types.ProcessID
	Msg         types.Message
}

type BroadcastNodeSelector func(ctx BroadcastContext) bool

func DefaultBroadcastNodeSelector() BroadcastNodeSelector {
	return func(ctx BroadcastContext) bool {
		return true
	}
}

func addToProcessesSlice(slice *[]types.ProcessID, p types.ProcessID) {
	for _, pp := range *slice {
		if pp == p {
			return
		}
	}
	*slice = append(*slice, p)
}

func removeFromProcessesSlice(slice *[]types.ProcessID, p types.ProcessID) {
	for i, pp := range *slice {
		if pp == p {
			*slice = append((*slice)[:i], (*slice)[i+1:]...)
			return
		}
	}
}
