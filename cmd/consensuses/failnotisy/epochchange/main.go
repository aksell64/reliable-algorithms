package main

import (
	"context"
	"reliable/broadcaster"
	"reliable/consensus/failnoisy/uniform"
	"reliable/database/inmemory"
	"reliable/election"
	"reliable/p2p"
	"time"

	"reliable/types"
	"reliable/utils"
)

func main() {

	ctx := context.Background()

	correct := utils.ProcessesIDRange(1, 11)
	processes := make(map[types.ProcessID]types.ProcessRank)
	for _, pid := range correct {
		processes[pid] = types.ProcessRank(pid)
	}

	nodes := make([]*uniform.LeaderBasedEpochChanger, 0, len(correct))

	for _, pid := range correct {
		nodes = append(nodes, makeEpochChanger(ctx, processes, pid))
	}

	for _, node := range nodes {
		node.Init()
	}

	for _, node := range nodes {
		node.Start()
	}

	select {}
}

func makeEpochChanger(
	ctx context.Context,
	processes map[types.ProcessID]types.ProcessRank,
	pid types.ProcessID,
) *uniform.LeaderBasedEpochChanger {
	fl := p2p.NewBaseLink(pid)
	sl := p2p.NewStubbornP2PLinks(ctx, fl)
	pl := p2p.NewPerfectP2PLinks(pid, sl)
	beb := broadcaster.NewBestEffortBroadcaster(pid, utils.KeysSlice(processes), broadcaster.DefaultBroadcastNodeSelector(), pl)
	store := inmemory.NewKVStore()
	elect := election.NewLowerEpochElection(ctx, pid, processes, store, fl, 1*time.Second, nil)
	ec := uniform.NewLeaderBasedEpochChanger(ctx, pid, processes, beb, elect, pl, nil)
	return ec
}
