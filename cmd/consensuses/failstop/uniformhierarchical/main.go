package main

import (
	"context"
	"reliable/broadcaster"
	"reliable/consensus"
	"reliable/consensus/failstop/uniformhierarchical"
	"reliable/p2p"
	"time"

	"reliable/failure"
	"reliable/types"
	"reliable/utils"
)

func main() {

	ctx := context.Background()

	correct := utils.ProcessesIDRange(1, 10)
	nodes := make([]consensus.Consensus, 0, len(correct))

	for _, pid := range correct {
		if pid != correct[len(correct)-1] {
			nodes = append(nodes, makeFailureConsensus(ctx, correct, pid))
			continue
		}
		//if pid == 1 /*|| pid == 2 || pid == 3 */ {
		//	nodes = append(nodes, makeFailureConsensus(ctx, correct, pid))
		//	continue
		//}

		nodes = append(nodes, makeConsensus(ctx, correct, pid))
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
) consensus.Consensus {
	baseLink := p2p.NewBaseLink(pid)
	sl := p2p.NewStubbornP2PLinks(ctx, baseLink)
	pl := p2p.NewPerfectP2PLinks(pid, sl)
	beb := broadcaster.NewBestEffortBroadcaster(pid, broadcaster.DefaultBroadcastNodeSelector(), pl)
	pfd := failure.NewPerfectFailureDetector(ctx, pid, corrects, pl, 3*time.Second)
	rb := broadcaster.NewLazyReliableBroadcaster(ctx, pid, beb, pfd)

	cfg := uniformhierarchical.Config{
		RoundInterval:    100 * time.Millisecond,
		DecisionInterval: 100 * time.Millisecond,
		ProposeInterval:  100 * time.Millisecond,
	}
	node := uniformhierarchical.New(ctx, pid, pl, rb, beb, pfd, cfg)
	return node
}

func makeFailureConsensus(
	ctx context.Context,
	corrects []types.ProcessID,
	pid types.ProcessID,
) consensus.Consensus {
	baseLink := p2p.NewBaseLink(pid)
	sl := p2p.NewStubbornP2PLinks(ctx, baseLink)
	pl := p2p.NewPerfectP2PLinks(pid, sl)
	beb := broadcaster.NewBestEffortBroadcaster(pid, broadcaster.DefaultBroadcastNodeSelector(), pl)
	pfd := failure.NewPerfectFailureDetector(ctx, pid, corrects, pl, 1*time.Second)
	rb := broadcaster.NewLazyReliableBroadcaster(ctx, pid, beb, pfd)

	cfg := uniformhierarchical.Config{
		RoundInterval:    100 * time.Millisecond,
		DecisionInterval: 100 * time.Millisecond,
		ProposeInterval:  100 * time.Millisecond,
		Runtime:          makeFailureRuntime(),
	}
	node := uniformhierarchical.New(ctx, pid, pl, rb, beb, pfd, cfg)
	return node
}

func makeFailureRuntime() *uniformhierarchical.Runtime {
	r := uniformhierarchical.NewRuntime()

	uniformhierarchical.AddHandler(r, func(evt uniformhierarchical.HandleAckEvt) uniformhierarchical.HandleResult {
		return uniformhierarchical.HandleResult{ShouldStop: true}
	})

	return r
}
