package main

import (
	"context"
	"reliable/broadcaster"
	"reliable/consensus"
	"reliable/consensus/coin"
	"reliable/consensus/coin/simple"
	"reliable/consensus/failsilent/randomized"
	"reliable/p2p"
	"reliable/types"
	"reliable/utils"
)

func main() {

	ctx := context.Background()

	processes := utils.ProcessesIDRange(1, 50)

	nodes := make([]consensus.Consensus, 0, len(processes))

	for _, pid := range processes {
		nodes = append(nodes, makeConsensus(ctx, processes, pid, 2))
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

	consensus.PrintDecided(consensus.MustUniqAgreement(consensus.CollectDecided(ctx, nodes...)))
}

func makeConsensus(
	ctx context.Context,
	processes []types.ProcessID,
	pid types.ProcessID,
	crashFaults int,
) consensus.Consensus {
	fl := p2p.NewBaseLink(pid)
	sl := p2p.NewStubbornP2PLinks(ctx, fl)
	pl := p2p.NewPerfectP2PLinks(pid, sl)

	beb1 := broadcaster.NewBestEffortBroadcaster(pid,
		processes,
		broadcaster.DefaultBroadcastNodeSelector(),
		pl)

	beb2 := broadcaster.NewBestEffortBroadcaster(pid,
		processes,
		broadcaster.DefaultBroadcastNodeSelector(),
		pl)

	rb := broadcaster.NewEagerReliableBroadcaster(ctx, pid, processes, beb1)

	coinFactory := func(domain ...types.Value) coin.CommonCoin {
		return simple.NewBiasedLocalRandCoin(domain...)
	}

	node := randomized.New(ctx, processes, pid, crashFaults, coinFactory, beb2, rb, nil)
	return node
}
