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
