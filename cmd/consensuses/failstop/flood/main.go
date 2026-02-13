package main

import (
	"context"
	"reliable/broadcaster"
	"reliable/consensus"
	"reliable/consensus/failstop/flooding"
	"reliable/p2p"
	"time"

	"reliable/failure"
	"reliable/types"
	"reliable/utils"
)

func main() {

	ctx := context.Background()
	selector := consensus.NewMinSelector()
	sig := utils.NewSig(ctx)

	correct := utils.ProcessesIDRange(1, 10)
	nodes := make([]consensus.Consensus, 0, len(correct))

	for _, pid := range correct {
		nodes = append(nodes, makeConsensus(ctx, correct, pid, selector, sig))
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
	corrects []types.ProcessID,
	pid types.ProcessID,
	selector consensus.DeterministicSelector,
	sig *utils.Sig,
) consensus.Consensus {
	baseLink := p2p.NewBaseLink(pid)
	stubbornLink := p2p.NewStubbornP2PLinks(ctx, baseLink)
	perfectLink := p2p.NewPerfectP2PLinks(pid, stubbornLink)
	beb := broadcaster.NewBestEffortBroadcaster(pid, broadcaster.DefaultBroadcastNodeSelector(), perfectLink)
	pfd := failure.NewPerfectFailureDetector(ctx, pid, corrects, perfectLink, time.Second)
	node := flooding.New(ctx, pid, selector, pfd, sig, beb)
	return node
}
