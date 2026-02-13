package main

import (
	"context"
	"reliable/broadcaster"
	"reliable/consensus"
	"reliable/consensus/failnoticy/uniform"
	"reliable/database/inmemory"
	"reliable/election"
	"reliable/p2p"
	"time"

	"reliable/types"
	"reliable/utils"
)

func main() {

	ctx := context.Background()

	correct := utils.ProcessesIDRange(1, 30)
	processes := make(map[types.ProcessID]types.ProcessRank)
	for _, pid := range correct {
		processes[pid] = types.ProcessRank(pid)
	}

	nodes := make([]consensus.Consensus, 0, len(correct))

	for _, pid := range correct {
		nodes = append(nodes, makeConsensus(ctx, processes, pid))
	}

	for _, node := range nodes {
		node.AddNodes(nodes...)
	}

	for _, node := range nodes {
		node.Init()
	}

	for _, node := range nodes {
		node.Start()
	}

	for i, node := range nodes {
		value := i*10 + 1
		go node.Propose(types.IntValue(value))
	}

	consensus.PrintDecided(consensus.CollectDecided(ctx, nodes...))
}

func makeConsensus(
	ctx context.Context,
	processes map[types.ProcessID]types.ProcessRank,
	pid types.ProcessID,
) consensus.Consensus {
	fl := p2p.NewBaseLink(pid)
	sl := p2p.NewStubbornP2PLinks(ctx, fl)
	pl := p2p.NewPerfectP2PLinks(pid, sl)
	beb := broadcaster.NewBestEffortBroadcaster(pid,
		utils.KeysSlice(processes),
		broadcaster.DefaultBroadcastNodeSelector(),
		pl)
	store := inmemory.NewKVStore()
	elect := election.NewLowerEpochElection(ctx, pid, processes, store, fl, 100*time.Millisecond, nil)
	ec := uniform.NewLeaderBasedEpochChanger(ctx, pid, processes, beb, elect, pl, nil)
	node := uniform.New(ctx, pid, ec, pl, beb)
	return node
}
