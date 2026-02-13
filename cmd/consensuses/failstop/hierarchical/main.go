package main

import (
	"context"
	"reliable/broadcaster"
	"reliable/consensus"
	"reliable/consensus/failstop/hierarchical"
	"reliable/p2p"
	"time"

	"reliable/failure"
	"reliable/types"
	"reliable/utils"
)

func main() {

	ctx := context.Background()

	correct := utils.ProcessesIDRange(1, 100)
	nodes := make([]consensus.Consensus, 0, len(correct))

	for i := range 100 {
		nodes = append(nodes, makeConsensus(ctx, correct, types.ProcessID(i+1)))
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

	select {}
}

func makeConsensus(
	ctx context.Context,
	corrects []types.ProcessID,
	pid types.ProcessID,
) consensus.Consensus {
	baseLink := p2p.NewBaseLink(pid)
	stubbornLink := p2p.NewStubbornP2PLinks(ctx, baseLink)
	perfectLink := p2p.NewPerfectP2PLinks(pid, stubbornLink)
	beb := broadcaster.NewBestEffortBroadcaster(pid, broadcaster.DefaultBroadcastNodeSelector(), perfectLink)
	pfd := failure.NewPerfectFailureDetector(ctx, pid, corrects, perfectLink, time.Second)

	conf := hierarchical.Config{
		BroadcastTickerInterval: time.Second,
		NextRoundTickerInterval: time.Second,
	}
	node := hierarchical.New(ctx, pid, types.ProcessRank(pid), pfd, conf, beb)
	return node
}
