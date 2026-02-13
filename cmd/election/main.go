package main

import (
	"context"
	"reliable/database/inmemory"
	"reliable/election"
	"reliable/p2p"
	"reliable/types"
	"reliable/utils"
	"sync/atomic"
	"time"
)

var oneFailure = atomic.Bool{}

func main() {
	ctx := context.Background()

	correct := utils.ProcessesIDRange(1, 10)
	processes := make(map[types.ProcessID]types.ProcessRank)
	for _, p := range correct {
		processes[p] = types.ProcessRank(p)
	}

	elections := make([]*election.LowerEpochElection, 0, len(correct))

	for _, p := range correct {
		elections = append(elections, makeElection(ctx, processes, p))
	}

	for _, e := range elections {
		e.Init()
	}

	for _, e := range elections {
		e.Start()
	}

	select {}
}

func makeElection(
	ctx context.Context,
	processes map[types.ProcessID]types.ProcessRank,
	self types.ProcessID) *election.LowerEpochElection {

	baseLink := p2p.NewBaseLink(self /*p2p.WithDeliverSleep(400*time.Millisecond, time.Second)*/)
	storage := inmemory.NewKVStore()

	e := election.NewLowerEpochElection(ctx, self, processes, storage, baseLink, 2000*time.Millisecond, nil)
	return e
}

func makeFailureElection(
	ctx context.Context,
	processes map[types.ProcessID]types.ProcessRank,
	self types.ProcessID,
) *election.LowerEpochElection {

	baseLink := p2p.NewBaseLink(self, p2p.WithDeliverSleep(2*time.Second, 3*time.Second))
	storage := inmemory.NewKVStore()

	rt := types.NewRuntime()

	start := time.Now()
	types.AddRuntimeHandler(rt, func(evt election.ElectionEvt) types.RuntimeHandleResult {
		if evt.CurrentLeader == evt.Self && time.Now().Sub(start) > 4*time.Second {
			if oneFailure.CompareAndSwap(false, true) {
				//return types.RuntimeHandleResult{ShouldStop: true}
			}
			return types.RuntimeHandleResult{ShouldStop: false}
		}
		return types.RuntimeHandleResult{}
	})

	e := election.NewLowerEpochElection(
		ctx,
		self,
		processes,
		storage,
		baseLink,
		100*time.Millisecond,
		types.NewRuntimeProcessor(ctx, rt))

	return e
}
